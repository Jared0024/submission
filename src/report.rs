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

pub fn build<'a, T: Stamp, S: Sale>(
    partitions: Vec<ShopperPartition<'a, T, S>>,
    dimensions: &Dimensions<'a>,
    people: usize,
    base: i64,
) -> (Vec<Vec<UserDay<'a>>>, CampaignDays) {
    let shards = partitions.len();
    let results: Vec<_> = partitions
        .into_par_iter()
        .enumerate()
        .map(|(shard, partition)| summarize(partition, dimensions, people, shard, shards, base))
        .collect();
    let mut users = Vec::new();
    let mut campaigns = CampaignDays::default();
    for (user_rows, campaign_rows) in results {
        users.push(user_rows);
        for (key, partial) in campaign_rows {
            let total = campaigns.entry(key).or_default();
            total.sessions += partial.sessions;
            total.purchasing_sessions += partial.purchasing_sessions;
            total.revenue += partial.revenue;
            total.assists += partial.assists;
        }
    }
    (users, campaigns)
}

fn summarize<'a, T: Stamp, S: Sale>(
    partition: ShopperPartition<'a, T, S>,
    dimensions: &Dimensions<'a>,
    people: usize,
    shard: usize,
    shards: usize,
    base: i64,
) -> (Vec<UserDay<'a>>, CampaignDays) {
    let groups = people.div_ceil(shards);
    let mut events = group(
        partition.events,
        groups,
        |row| row.person as usize / shards,
        |row| row.activity,
    );
    let mut clicks = group(
        partition.clicks,
        groups,
        |row| row.person as usize / shards,
        |row| row,
    );

    let mut users = Vec::new();
    let mut campaigns = CampaignDays::default();
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
            let summary = campaigns.entry((credited, day)).or_default();
            summary.sessions += 1;
            summary.purchasing_sessions += i64::from(revenue > 0);
            summary.revenue += revenue;
            for &campaign in &window.active {
                campaigns.entry((campaign, day)).or_default().assists += 1;
            }
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
