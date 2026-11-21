use std::cmp::Reverse;

use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::{
    input::{DAY_MS, Dimensions, GAP_MS, Id, WEEK_MS},
    partition::{Activity, Click, Sale, ShopperPartition, Stamp},
};

pub struct UserDay<'a> {
    pub person: u32,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top_category: Option<Id<'a>>,
}

#[derive(Default)]
pub struct CampaignTotals {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue: i64,
    pub assists: i64,
}

pub type CampaignDays = FxHashMap<(usize, i32), CampaignTotals>;

#[derive(Clone, Copy)]
struct Grid {
    first_day: i32,
    campaigns: usize,
    cells: usize,
}

impl Grid {
    fn new(window: std::ops::Range<i64>, campaigns: usize, shards: usize) -> Option<Self> {
        if window.is_empty() {
            return None;
        }
        let first_day = i32::try_from(window.start.div_euclid(DAY_MS)).ok()?;
        let last_day = i32::try_from((window.end - 1).div_euclid(DAY_MS)).ok()?;
        let days = usize::try_from(i64::from(last_day) - i64::from(first_day) + 1).ok()?;
        let cells = days.checked_mul(campaigns)?;
        // All shards can finish before the final merge. Bound their combined
        // dense storage, including the separate eight-byte assist counters.
        let budget = (32 << 20) / size_of::<CampaignTotals>() / shards;
        (cells <= budget).then_some(Self {
            first_day,
            campaigns,
            cells,
        })
    }
}

#[derive(Clone, Copy, Default)]
struct Credit {
    sessions: i64,
    purchases: i64,
    revenue: i64,
}

enum CampaignPart {
    Dense {
        grid: Grid,
        credit: Vec<Credit>,
        assists: Vec<i64>,
    },
    Sparse(CampaignDays),
}

impl CampaignPart {
    fn new(grid: Option<Grid>) -> Self {
        match grid {
            Some(grid) => Self::Dense {
                grid,
                credit: vec![Credit::default(); grid.cells],
                assists: vec![0; grid.cells],
            },
            None => Self::Sparse(CampaignDays::default()),
        }
    }

    fn record(&mut self, campaign: usize, day: i32, revenue: i64, active: &[usize]) {
        match self {
            Self::Dense {
                grid,
                credit,
                assists,
            } => {
                let start = (i64::from(day) - i64::from(grid.first_day)) as usize * grid.campaigns;
                let row = &mut credit[start + campaign];
                row.sessions += 1;
                row.purchases += i64::from(revenue > 0);
                row.revenue += revenue;
                let day_assists = &mut assists[start..start + grid.campaigns];
                for &id in active {
                    day_assists[id] += 1;
                }
            }
            Self::Sparse(rows) => {
                let row = rows.entry((campaign, day)).or_default();
                row.sessions += 1;
                row.purchasing_sessions += i64::from(revenue > 0);
                row.revenue += revenue;
                for &id in active {
                    rows.entry((id, day)).or_default().assists += 1;
                }
            }
        }
    }

    fn combine(parts: Vec<Self>) -> CampaignDays {
        let mut parts = parts.into_iter();
        let Some(mut total) = parts.next() else {
            return CampaignDays::default();
        };
        // Every shopper shard shares the reporting window and catalog.
        for part in parts {
            match (&mut total, part) {
                (
                    Self::Dense {
                        credit, assists, ..
                    },
                    Self::Dense {
                        credit: other,
                        assists: counts,
                        ..
                    },
                ) => {
                    for (row, add) in credit.iter_mut().zip(other) {
                        row.sessions += add.sessions;
                        row.purchases += add.purchases;
                        row.revenue += add.revenue;
                    }
                    for (row, add) in assists.iter_mut().zip(counts) {
                        *row += add;
                    }
                }
                (Self::Sparse(rows), Self::Sparse(other)) => {
                    for (key, add) in other {
                        let row = rows.entry(key).or_default();
                        row.sessions += add.sessions;
                        row.purchasing_sessions += add.purchasing_sessions;
                        row.revenue += add.revenue;
                        row.assists += add.assists;
                    }
                }
                _ => unreachable!("shards share a single campaign grid"),
            }
        }
        match total {
            Self::Sparse(rows) => rows,
            Self::Dense {
                grid,
                credit,
                assists,
            } => credit
                .into_iter()
                .zip(assists)
                .enumerate()
                .filter(|(_, (row, assists))| row.sessions != 0 || *assists != 0)
                .map(|(cell, (row, assists))| {
                    let day = i64::from(grid.first_day) + (cell / grid.campaigns) as i64;
                    (
                        (cell % grid.campaigns, day as i32),
                        CampaignTotals {
                            sessions: row.sessions,
                            purchasing_sessions: row.purchases,
                            revenue: row.revenue,
                            assists,
                        },
                    )
                })
                .collect(),
        }
    }
}

pub fn build<'a, T: Stamp, S: Sale>(
    partitions: Vec<ShopperPartition<'a, T, S>>,
    dimensions: &Dimensions<'a>,
    people: usize,
    base: i64,
    window: std::ops::Range<i64>,
) -> (Vec<Vec<UserDay<'a>>>, CampaignDays) {
    let shards = partitions.len();
    let grid = Grid::new(window, dimensions.campaign_names.len(), shards).filter(|grid| {
        // Each shard clears and later merges a full grid. On sparse inputs a
        // map costs less than visiting more grid cells than surviving events.
        let events: usize = partitions
            .iter()
            .flat_map(|p| &p.events)
            .map(Vec::len)
            .sum();
        grid.cells <= events / shards
    });
    let results: Vec<_> = partitions
        .into_par_iter()
        .enumerate()
        .map(|(shard, partition)| {
            summarize(partition, dimensions, people, shard, shards, base, grid)
        })
        .collect();
    let (users, partials): (Vec<_>, Vec<_>) = results.into_iter().unzip();
    (users, CampaignPart::combine(partials))
}

fn summarize<'a, T: Stamp, S: Sale>(
    partition: ShopperPartition<'a, T, S>,
    dimensions: &Dimensions<'a>,
    people: usize,
    shard: usize,
    shards: usize,
    base: i64,
    grid: Option<Grid>,
) -> (Vec<UserDay<'a>>, CampaignPart) {
    debug_assert!(shards.is_power_of_two());
    let shard_bits = shards.trailing_zeros();
    let groups = people.div_ceil(shards);
    let mut events = group(
        partition.events,
        groups,
        |row| row.person as usize >> shard_bits,
        |row| row.activity,
    );
    let mut clicks = group(
        partition.clicks,
        groups,
        |row| row.person as usize >> shard_bits,
        |row| row,
    );

    let mut users = Vec::new();
    let mut campaigns = CampaignPart::new(grid);
    let mut categories = CategoryTotals::new(dimensions.categories.len());
    let organic = dimensions.campaign_names.len() - 1;
    let mut window = ClickWindow::new(dimensions.campaign_names.len());
    for (local, (events, clicks)) in events.groups_mut().zip(clicks.groups_mut()).enumerate() {
        if events.is_empty() {
            continue;
        }
        let person = (local * shards + shard) as u32;
        events.sort_unstable_by_key(|row| row.time);
        // The last eligible click wins, with the smallest bytewise ID on ties.
        clicks.sort_unstable_by_key(|row| (row.time, Reverse(row.id)));
        window.reset();

        let mut current_day: Option<UserDay<'a>> = None;
        for session in events.chunk_by(|earlier, later| {
            later
                .time
                .millis(base)
                .saturating_sub(earlier.time.millis(base))
                <= GAP_MS
        }) {
            let start = session[0].time.millis(base);
            let day = start.div_euclid(DAY_MS) as i32;
            if current_day.as_ref().is_some_and(|row| row.day != day)
                && let Some(mut row) = current_day.take()
            {
                row.top_category = categories.finish_day(&dimensions.categories);
                users.push(row);
            }

            let current = current_day.get_or_insert(UserDay {
                person,
                day,
                sessions: 0,
                events: 0,
                revenue: 0,
                top_category: None,
            });
            current.sessions += 1;
            current.events += session.len() as i64;
            let revenue = categories.add_purchases(session, &dimensions.categories);
            current.revenue += revenue;

            let credited = window.advance(clicks, start, base).unwrap_or(organic);
            campaigns.record(credited, day, revenue, &window.active);
        }
        if let Some(mut last) = current_day {
            last.top_category = categories.finish_day(&dimensions.categories);
            users.push(last);
        }
    }
    (users, campaigns)
}

struct Groups<T> {
    rows: Vec<T>,
    ends: Vec<usize>,
}

impl<T> Groups<T> {
    fn groups_mut(&mut self) -> impl Iterator<Item = &mut [T]> {
        let mut remaining = self.rows.as_mut_slice();
        let mut start = 0;
        self.ends.iter().map(move |&end| {
            let (group, rest) = std::mem::take(&mut remaining).split_at_mut(end - start);
            remaining = rest;
            start = end;
            group
        })
    }
}

// Scatter directly from producer buffers into disjoint shopper ranges. Only
// timestamps inside a single shopper's range need comparison sorting.
fn group<R, T: Copy>(
    parts: Vec<Vec<R>>,
    groups: usize,
    owner: impl Fn(&R) -> usize,
    value: impl Fn(R) -> T,
) -> Groups<T> {
    let mut ends = vec![0; groups];
    for row in parts.iter().flatten() {
        ends[owner(row)] += 1;
    }
    let mut total = 0;
    let mut next: Vec<_> = ends
        .iter_mut()
        .map(|end| {
            let start = total;
            total += *end;
            *end = total;
            start
        })
        .collect();
    let mut rows: Vec<T> = Vec::with_capacity(total);
    let spare = rows.spare_capacity_mut();
    for row in parts.into_iter().flatten() {
        let id = owner(&row);
        let dest = next[id];
        // Each range advances independently and cannot overwrite another.
        assert!(dest < ends[id]);
        spare[dest].write(value(row));
        next[id] += 1;
    }
    // SAFETY: the cursors advanced exactly total times within disjoint
    // ranges of combined length total. Each exposed slot was written once.
    unsafe {
        rows.set_len(total);
    }
    Groups { rows, ends }
}

struct CategoryTotals {
    revenue: Vec<i64>,
    touched: Vec<usize>,
    top: Option<usize>,
}

impl CategoryTotals {
    fn new(size: usize) -> Self {
        Self {
            revenue: vec![0; size],
            touched: Vec::new(),
            top: None,
        }
    }

    fn add_purchases<T, S: Sale>(&mut self, events: &[Activity<T, S>], names: &[Id<'_>]) -> i64 {
        let mut total = 0;
        for event in events.iter().filter(|event| event.sale != S::default()) {
            let cents = i64::from(event.sale.cents());
            total += cents;
            let category = event.sale.category();
            if self.revenue[category] == 0 {
                self.touched.push(category);
            }
            self.revenue[category] += cents;
            if self.top.is_none_or(|best| {
                self.revenue[category] > self.revenue[best]
                    || (self.revenue[category] == self.revenue[best]
                        && names[category] < names[best])
            }) {
                self.top = Some(category);
            }
        }
        total
    }

    fn finish_day<'a>(&mut self, names: &[Id<'a>]) -> Option<Id<'a>> {
        for category in self.touched.drain(..) {
            self.revenue[category] = 0;
        }
        self.top.take().map(|index| names[index])
    }
}

struct ClickWindow {
    first: usize,
    next: usize,
    count: Vec<usize>,
    position: Vec<usize>,
    active: Vec<usize>,
}

impl ClickWindow {
    fn new(size: usize) -> Self {
        Self {
            first: 0,
            next: 0,
            count: vec![0; size],
            position: vec![0; size],
            active: Vec::new(),
        }
    }

    fn reset(&mut self) {
        for campaign in self.active.drain(..) {
            self.count[campaign] = 0;
        }
        self.first = 0;
        self.next = 0;
    }

    fn advance<T: Stamp>(
        &mut self,
        clicks: &[Click<'_, T>],
        start: i64,
        base: i64,
    ) -> Option<usize> {
        // Repeated click IDs are identical by the spec. Counting copies in
        // the window gives the same active set and latest-click winner.
        while self.next < clicks.len() && clicks[self.next].time.millis(base) <= start {
            let campaign = clicks[self.next].campaign;
            if self.count[campaign] == 0 {
                self.position[campaign] = self.active.len();
                self.active.push(campaign);
            }
            self.count[campaign] += 1;
            self.next += 1;
        }
        while self.first < self.next
            && clicks[self.first].time.millis(base) < start.saturating_sub(WEEK_MS)
        {
            let campaign = clicks[self.first].campaign;
            self.count[campaign] -= 1;
            if self.count[campaign] == 0 {
                let position = self.position[campaign];
                self.active.swap_remove(position);
                if let Some(&moved) = self.active.get(position) {
                    self.position[moved] = position;
                }
            }
            self.first += 1;
        }
        (self.first < self.next).then(|| clicks[self.next - 1].campaign)
    }
}

#[cfg(test)]
mod tests {
    use super::group;

    #[test]
    fn campaign_grid_and_sparse_merge_agree_across_midnight_and_assists() {
        use super::{CampaignPart, DAY_MS, Grid};

        let grid = Grid::new(-1..DAY_MS, 3, 8).unwrap();
        assert_eq!(grid.first_day, -1);
        assert_eq!(grid.cells, 6);
        assert!(Grid::new(0..0, 3, 8).is_none());
        assert!(Grid::new(0..365 * DAY_MS, 100_000, 8).is_none());

        for layout in [Some(grid), None] {
            let mut a = CampaignPart::new(layout);
            let mut b = CampaignPart::new(layout);
            a.record(2, -1, 0, &[]);
            a.record(1, 0, 12, &[0, 1]);
            b.record(1, 0, 0, &[0]);
            let combined = CampaignPart::combine(vec![a, b]);
            let row = &combined[&(1, 0)];
            assert_eq!(
                (
                    row.sessions,
                    row.purchasing_sessions,
                    row.revenue,
                    row.assists
                ),
                (2, 1, 12, 1)
            );
            assert_eq!(
                (combined[&(0, 0)].sessions, combined[&(0, 0)].assists),
                (0, 2)
            );
            assert_eq!(combined[&(2, -1)].sessions, 1);
            assert_eq!(combined.len(), 3);
        }
    }

    #[test]
    fn scatter_initializes_disjoint_ranges_including_empty_shoppers() {
        let parts = vec![vec![(3, 7), (1, 0)], vec![], vec![(3, 9), (3, 2), (1, 6)]];
        let mut groups = group(parts, 5, |row| row.0, |row| row.1);
        let values: Vec<_> = groups.groups_mut().map(|rows| rows.to_vec()).collect();
        assert_eq!(values, [vec![], vec![0, 6], vec![], vec![7, 9, 2], vec![]]);
        assert!(
            group::<u32, u32>(vec![], 0, |_| 0, |row| row)
                .rows
                .is_empty()
        );
    }
}
