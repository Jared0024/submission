use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use parquet::{
    basic::Compression,
    data_type::{ByteArray, ByteArrayType, DataType, Int32Type, Int64Type},
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::parser::parse_message_type,
};

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
    people: &[Box<[u8]>],
    names: &[(Id<'_>, Id<'_>)],
) -> Result<()> {
    let (user_result, campaign_result) = rayon::join(
        || {
            write_table(output, "user_daily", USER, |writer| {
                for partition in users {
                    for batch in partition.chunks(ROW_GROUP) {
                        let mut group = writer.next_row_group()?;
                        column::<ByteArrayType>(
                            &mut group,
                            &batch
                                .iter()
                                .map(|r| bytes(&people[r.person as usize]))
                                .collect::<Vec<_>>(),
                            None,
                        )?;
                        column::<Int32Type>(
                            &mut group,
                            &batch.iter().map(|r| r.day).collect::<Vec<_>>(),
                            None,
                        )?;
                        column::<Int64Type>(
                            &mut group,
                            &batch.iter().map(|r| r.sessions).collect::<Vec<_>>(),
                            None,
                        )?;
                        column::<Int64Type>(
                            &mut group,
                            &batch.iter().map(|r| r.events).collect::<Vec<_>>(),
                            None,
                        )?;
                        column::<Int64Type>(
                            &mut group,
                            &batch.iter().map(|r| r.revenue).collect::<Vec<_>>(),
                            None,
                        )?;
                        let level: Vec<i16> = batch
                            .iter()
                            .map(|r| i16::from(r.top_category.is_some()))
                            .collect();
                        let category: Vec<_> = batch
                            .iter()
                            .filter_map(|r| r.top_category.map(bytes))
                            .collect();
                        column::<ByteArrayType>(&mut group, &category, Some(&level))?;
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
