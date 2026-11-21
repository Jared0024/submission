use std::{
    borrow::Cow,
    fs::{self, File},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use parquet::{
    basic::Compression,
    column::writer::{ColumnCloseResult, ColumnWriterImpl},
    data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
    file::{
        properties::{WriterProperties, WriterPropertiesPtr},
        writer::{
            SerializedFileWriter, SerializedPageWriter, SerializedRowGroupWriter, TrackedWrite,
        },
    },
    schema::{
        parser::parse_message_type,
        types::{ColumnDescPtr, SchemaDescriptor},
    },
};
use rayon::prelude::*;

use crate::{
    input::Id,
    report::{CampaignDays, UserDay},
};

const USER: &str = "
    message user_daily {
        REQUIRED BYTE_ARRAY user_id (UTF8);
        REQUIRED INT32 day (DATE);
        REQUIRED INT64 sessions;
        REQUIRED INT64 events;
        REQUIRED INT64 revenue_cents;
        OPTIONAL BYTE_ARRAY top_category (UTF8);
    }
";

const CAMPAIGN: &str = "
    message campaign_daily {
        REQUIRED BYTE_ARRAY campaign_id (UTF8);
        REQUIRED BYTE_ARRAY channel (UTF8);
        REQUIRED INT32 day (DATE);
        REQUIRED INT64 sessions;
        REQUIRED INT64 purchasing_sessions;
        REQUIRED INT64 revenue_cents;
        REQUIRED INT64 assisted_sessions;
    }
";

const ROW_GROUP: usize = 65_536;

pub fn write_all(
    output: &Path,
    users: Vec<Vec<UserDay<'_>>>,
    campaigns: CampaignDays,
    people: &[Cow<'_, [u8]>],
    names: &[(Id<'_>, Id<'_>)],
) -> Result<()> {
    let (user_result, campaign_result) = rayon::join(
        || {
            write_table(output, "user_daily", USER, |writer| {
                let batches: Vec<_> = users
                    .iter()
                    .flat_map(|rows| rows.chunks(ROW_GROUP))
                    .collect();
                // Limit buffered groups to one per worker. Append in order to keep a
                // single output file and preserve atomic replacement on a rerun.
                for wave in batches.chunks(rayon::current_num_threads()) {
                    let schema = writer.schema_descr();
                    let props = writer.properties();
                    let groups: Vec<_> = wave
                        .par_iter()
                        .map(|rows| encode_users(rows, people, schema, props))
                        .collect::<Result<_>>()?;
                    for columns in groups {
                        let mut group = writer.next_row_group()?;
                        for (bytes, metadata) in columns {
                            group.append_column(&bytes, metadata)?;
                        }
                        group.close()?;
                    }
                }
                Ok(())
            })
        },
        || {
            write_table(output, "campaign_daily", CAMPAIGN, |writer| {
                let mut rows: Vec<_> = campaigns.into_iter().collect();
                rows.sort_unstable_by_key(|&((campaign, day), _)| (campaign, day));
                for batch in rows.chunks(ROW_GROUP) {
                    let mut group = writer.next_row_group()?;
                    column::<ByteArrayType>(
                        &mut group,
                        &batch
                            .iter()
                            .map(|&((id, _), _)| bytes(names[id].0))
                            .collect::<Vec<_>>(),
                        None,
                    )?;
                    column::<ByteArrayType>(
                        &mut group,
                        &batch
                            .iter()
                            .map(|&((id, _), _)| bytes(names[id].1))
                            .collect::<Vec<_>>(),
                        None,
                    )?;
                    column::<Int32Type>(
                        &mut group,
                        &batch.iter().map(|&((_, day), _)| day).collect::<Vec<_>>(),
                        None,
                    )?;
                    column::<Int64Type>(
                        &mut group,
                        &batch.iter().map(|(_, r)| r.sessions).collect::<Vec<_>>(),
                        None,
                    )?;
                    column::<Int64Type>(
                        &mut group,
                        &batch
                            .iter()
                            .map(|(_, r)| r.purchasing_sessions)
                            .collect::<Vec<_>>(),
                        None,
                    )?;
                    column::<Int64Type>(
                        &mut group,
                        &batch.iter().map(|(_, r)| r.revenue).collect::<Vec<_>>(),
                        None,
                    )?;
                    column::<Int64Type>(
                        &mut group,
                        &batch.iter().map(|(_, r)| r.assists).collect::<Vec<_>>(),
                        None,
                    )?;
                    group.close()?;
                }
                Ok(())
            })
        },
    );
    user_result?;
    campaign_result?;
    Ok(())
}

type EncodedColumn = (Bytes, ColumnCloseResult);

fn encode_users(
    rows: &[UserDay<'_>],
    people: &[Cow<'_, [u8]>],
    schema: &SchemaDescriptor,
    props: &WriterPropertiesPtr,
) -> Result<[EncodedColumn; 6]> {
    let levels: Vec<_> = rows
        .iter()
        .map(|r| i16::from(r.top_category.is_some()))
        .collect();
    Ok([
        encode::<ByteArrayType>(
            schema.column(0),
            props,
            rows.iter().map(|r| bytes(&people[r.person as usize])),
            None,
        )?,
        encode::<Int32Type>(schema.column(1), props, rows.iter().map(|r| r.day), None)?,
        encode::<Int64Type>(
            schema.column(2),
            props,
            rows.iter().map(|r| r.sessions),
            None,
        )?,
        encode::<Int64Type>(schema.column(3), props, rows.iter().map(|r| r.events), None)?,
        encode::<Int64Type>(
            schema.column(4),
            props,
            rows.iter().map(|r| r.revenue),
            None,
        )?,
        encode::<ByteArrayType>(
            schema.column(5),
            props,
            rows.iter().filter_map(|r| r.top_category.map(bytes)),
            Some(&levels),
        )?,
    ])
}

fn encode<T: DataType>(
    desc: ColumnDescPtr,
    props: &WriterPropertiesPtr,
    values: impl Iterator<Item = T::T>,
    levels: Option<&[i16]>,
) -> Result<EncodedColumn> {
    let mut sink = TrackedWrite::new(Vec::new());
    let pages = Box::new(SerializedPageWriter::new(&mut sink));
    let mut column = ColumnWriterImpl::<T>::new(desc, props.clone(), pages);
    column.write_batch(&values.collect::<Vec<_>>(), levels, None)?;
    let close = column.close()?;
    Ok((Bytes::from(sink.into_inner()?), close))
}

fn bytes(value: Id<'_>) -> ByteArray {
    ByteArray::from(value.to_vec())
}

fn column<T: DataType>(
    group: &mut SerializedRowGroupWriter<'_, File>,
    values: &[T::T],
    levels: Option<&[i16]>,
) -> Result<()> {
    let mut column = group
        .next_column()?
        .context("Parquet column missing from schema")?;
    column.typed::<T>().write_batch(values, levels, None)?;
    column.close()?;
    Ok(())
}

fn write_table(
    output: &Path,
    name: &str,
    schema: &str,
    write: impl FnOnce(&mut SerializedFileWriter<File>) -> Result<()>,
) -> Result<()> {
    let directory = output.join(name);
    fs::create_dir_all(&directory)?;
    let temporary = directory.join(format!(".part-0-{}.tmp", std::process::id()));
    let file = File::create(&temporary)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = SerializedFileWriter::new(
        file,
        Arc::new(parse_message_type(schema)?),
        Arc::new(properties),
    )?;
    write(&mut writer)?;
    writer.close()?;
    fs::rename(temporary, directory.join("part-0.parquet"))?;
    Ok(())
}
