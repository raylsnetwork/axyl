// SPDX-License-Identifier: BUSL-1.1
//! Plain-text rendering: aligned columns, one table per report, one-line verdict last.

use crate::report::{
    epoch::{describe_position, ChainCheckReport, EpochReport, EpochStatus, EpochsReport},
    header::{describe_absent, CertReport, HeaderReport, WalkReport},
    summary::SummaryReport,
    Report,
};
use std::fmt::Write as _;

/// Minimal aligned-column table.
#[derive(Debug, Default)]
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new(headers: &[&str]) -> Self {
        Self { headers: headers.iter().map(|h| h.to_string()).collect(), rows: Vec::new() }
    }

    pub fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    pub fn render(&self) -> String {
        let cols = self.headers.len().max(self.rows.iter().map(Vec::len).max().unwrap_or(0));
        let mut widths = vec![0usize; cols];
        for row in std::iter::once(&self.headers).chain(&self.rows) {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        let mut out = String::new();
        let line = |cells: &[String], out: &mut String| {
            let mut parts = Vec::with_capacity(cells.len());
            for (i, cell) in cells.iter().enumerate() {
                let pad = if i + 1 == cells.len() { 0 } else { widths[i] - cell.chars().count() };
                parts.push(format!("{cell}{}", " ".repeat(pad)));
            }
            let _ = writeln!(out, "{}", parts.join("  ").trim_end());
        };
        line(&self.headers, &mut out);
        let _ = writeln!(
            out,
            "{}",
            widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  ")
        );
        for row in &self.rows {
            line(row, &mut out);
        }
        out
    }
}

fn opt<T: std::fmt::Display>(v: &Option<T>) -> String {
    v.as_ref().map(|v| v.to_string()).unwrap_or_else(|| "-".to_owned())
}

fn yes_no(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "NO"
    }
}

fn opt_yes_no(b: Option<bool>) -> String {
    b.map(|b| yes_no(b).to_owned()).unwrap_or_else(|| "-".to_owned())
}

/// Renders `report` as text.
pub fn render(report: &Report) -> String {
    let mut out = String::new();
    match report {
        Report::Epoch(r) => render_epoch(r, &mut out),
        Report::Epochs(r) => render_epochs(r, &mut out),
        Report::ChainCheck(r) => render_chain_check(r, &mut out),
        Report::Header(r) => render_header(r, &mut out),
        Report::Cert(r) => render_cert(r, &mut out),
        Report::Walk(r) => render_walk(r, &mut out),
        Report::Summary(r) => render_summary(r, &mut out),
    }
    if let Some(v) = report.verdict() {
        let _ = writeln!(out, "\nverdict: {v}");
    }
    out
}

fn render_epoch(r: &EpochReport, out: &mut String) {
    let _ = writeln!(out, "epoch {}", r.epoch);
    let mut t = Table::new(&[
        "node",
        "live",
        "status",
        "record digest",
        "index",
        "cert",
        "signers",
        "quorum",
        "sig",
        "parent link",
        "handoff",
        "boundary #",
        "checkpoint",
    ]);
    for n in &r.nodes {
        let rec = n.record.as_ref();
        let cert = n.cert.as_ref();
        t.row(vec![
            n.node.clone(),
            n.live.to_string(),
            if n.status == EpochStatus::NotReached {
                format!("not reached ({})", describe_position(n.position))
            } else {
                n.status.to_string()
            },
            rec.map(|r| r.digest.clone()).unwrap_or_else(|| "-".to_owned()),
            rec.map(|r| yes_no(r.index_ok).to_owned()).unwrap_or_else(|| "-".to_owned()),
            match cert {
                Some(c) if c.valid => "valid".to_owned(),
                Some(c) if !c.digest_match => "DIGEST MISMATCH".to_owned(),
                Some(_) => "INVALID".to_owned(),
                None if rec.is_some() && r.epoch == 0 => "n/a (genesis)".to_owned(),
                None if rec.is_some() => "MISSING".to_owned(),
                None => "-".to_owned(),
            },
            match (cert, rec) {
                (Some(c), Some(r)) => {
                    format!("{}/{} (need {})", c.signer_count, r.committee_size, r.super_quorum)
                }
                _ => "-".to_owned(),
            },
            cert.map(|c| yes_no(c.quorum_ok).to_owned()).unwrap_or_else(|| "-".to_owned()),
            cert.map(|c| yes_no(c.signature_ok).to_owned()).unwrap_or_else(|| "-".to_owned()),
            rec.map(|r| r.parent_link.to_string()).unwrap_or_else(|| "-".to_owned()),
            rec.map(|r| opt_yes_no(r.committee_handoff_ok)).unwrap_or_else(|| "-".to_owned()),
            rec.map(|r| match (r.parent_consensus_number, r.parent_consensus_tier) {
                (Some(n), Some(tier)) => format!("{n} ({tier})"),
                (Some(n), None) => format!("{n} (indexed, header missing)"),
                (None, _) => "unresolved".to_owned(),
            })
            .unwrap_or_else(|| "-".to_owned()),
            n.checkpoint
                .as_ref()
                .map(|c| format!("LEFTOVER {}", c.completed_phase))
                .unwrap_or_else(|| "none".to_owned()),
        ]);
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(rec) = &n.record else { continue };
        let _ = writeln!(out, "\n[{}]", n.node);
        let _ = writeln!(out, "  parent_hash       {}", rec.parent_hash);
        let _ = writeln!(out, "  parent_consensus  {}", rec.parent_consensus);
        let _ = writeln!(
            out,
            "  parent_state      #{} {}",
            rec.parent_state_number, rec.parent_state_hash
        );
        let _ = writeln!(
            out,
            "  committee         {} keys, next {} keys",
            rec.committee_size, rec.next_committee_size
        );
        if let Some(c) = &n.cert {
            let _ = writeln!(out, "  cert epoch_hash   {}", c.epoch_hash);
            let _ = writeln!(out, "  cert signers      {:?}", c.signers);
            let _ = writeln!(out, "  cert signature    {}", c.signature);
        }
        if let Some(cp) = &n.checkpoint {
            let _ = writeln!(
                out,
                "  checkpoint        epoch {} phase {} target {} at {}",
                cp.epoch, cp.completed_phase, cp.target_hash, cp.timestamp
            );
        }
        if let Some(keys) = &rec.committee {
            let _ = writeln!(out, "  committee keys:");
            for (i, k) in keys.iter().enumerate() {
                let _ = writeln!(out, "    [{i}] {k}");
            }
        }
        if let Some(keys) = &rec.next_committee {
            let _ = writeln!(out, "  next committee keys:");
            for (i, k) in keys.iter().enumerate() {
                let _ = writeln!(out, "    [{i}] {k}");
            }
        }
    }
}

fn render_epochs(r: &EpochsReport, out: &mut String) {
    let _ = writeln!(
        out,
        "epochs {}..={}  RC record+cert  R- record only  -- missing  .. not reached  ?? no table",
        r.from, r.to
    );
    let mut headers = vec!["epoch"];
    headers.extend(r.nodes.iter().map(String::as_str));
    headers.push("status");
    let mut t = Table::new(&headers);
    for row in &r.rows {
        let mut cells = vec![row.epoch.to_string()];
        cells.extend(row.cells.iter().map(|c| c.glyph().to_owned()));
        cells.push(row.status.to_owned());
        t.row(cells);
    }
    out.push_str(&t.render());
}

fn render_chain_check(r: &ChainCheckReport, out: &mut String) {
    let _ = writeln!(out, "epoch chain check");
    let mut t = Table::new(&[
        "node",
        "live",
        "range",
        "checked",
        "certified",
        "gaps",
        "broken links",
        "uncertified",
        "invalid certs",
        "handoff mismatch",
        "ok",
    ]);
    for n in &r.nodes {
        t.row(vec![
            n.node.clone(),
            n.live.to_string(),
            match (n.from, n.to) {
                (Some(f), Some(t)) => format!("{f}..={t}"),
                _ if n.note.is_some() => "n/a".to_owned(),
                _ => "no records".to_owned(),
            },
            n.checked.to_string(),
            n.certified.to_string(),
            list(&n.gaps),
            list(&n.broken_links.iter().map(|b| b.epoch).collect::<Vec<_>>()),
            list(&n.uncertified),
            list(&n.invalid_certs),
            list(&n.committee_handoff_mismatch),
            if n.checked == 0 { "-".to_owned() } else { yes_no(n.ok).to_owned() },
        ]);
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        if let Some(note) = &n.note {
            let _ = writeln!(out, "\n[{}] {note}", n.node);
        }
        for b in &n.broken_links {
            let _ = writeln!(
                out,
                "\n[{}] epoch {}: parent_hash {} expected {}",
                n.node, b.epoch, b.parent_hash, b.expected
            );
        }
    }
}

fn list<T: std::fmt::Display>(items: &[T]) -> String {
    const MAX: usize = 8;
    if items.is_empty() {
        return "none".to_owned();
    }
    let shown: Vec<String> = items.iter().take(MAX).map(|i| i.to_string()).collect();
    if items.len() > MAX {
        format!("{} (+{} more)", shown.join(","), items.len() - MAX)
    } else {
        shown.join(",")
    }
}

fn render_header(r: &HeaderReport, out: &mut String) {
    let _ = writeln!(out, "consensus header {}", r.number);
    let mut t = Table::new(&[
        "node",
        "live",
        "tier",
        "digest",
        "parent_hash",
        "leader round",
        "leader epoch",
        "certs",
        "batches",
        "commit ts",
    ]);
    for n in &r.nodes {
        match &n.header {
            Some(h) => t.row(vec![
                n.node.clone(),
                n.live.to_string(),
                opt(&n.tier),
                h.digest.clone(),
                h.parent_hash.clone(),
                h.leader.round.to_string(),
                h.leader.epoch.to_string(),
                h.certificate_count.to_string(),
                h.batch_count.to_string(),
                h.commit_timestamp.to_string(),
            ]),
            None => {
                t.row(vec![n.node.clone(), n.live.to_string(), describe_absent(n.lookup, n.tip)])
            }
        }
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(h) = &n.header else { continue };
        let _ = writeln!(out, "\n[{}]", n.node);
        let _ = writeln!(out, "  leader        {} by {}", h.leader.digest, h.leader.author);
        let _ = writeln!(out, "  extra         {}", h.extra);
        let _ =
            writeln!(out, "  reputation    final_of_schedule={}", h.reputation_final_of_schedule);
        if let Some(certs) = &h.certificates {
            let _ = writeln!(out, "  sub-dag certificates:");
            for c in certs {
                let _ = writeln!(
                    out,
                    "    {} r{} e{} by {} signers={}",
                    c.summary.digest,
                    c.summary.round,
                    c.summary.epoch,
                    c.summary.author,
                    c.signer_count
                );
            }
        }
        if let Some(batches) = &h.batches {
            let _ = writeln!(out, "  batches:");
            for b in batches {
                let _ = writeln!(
                    out,
                    "    {} {}",
                    b.digest,
                    b.tier.map(|t| t.to_string()).unwrap_or_else(|| "MISSING".to_owned())
                );
            }
        }
        if let Some(rep) = &h.reputation {
            let _ = writeln!(out, "  reputation scores:");
            for (id, score) in rep {
                let _ = writeln!(out, "    {id} {score}");
            }
        }
    }
}

fn render_cert(r: &CertReport, out: &mut String) {
    let _ = writeln!(out, "leader cert of header {}", r.number);
    let mut t = Table::new(&[
        "node",
        "live",
        "tier",
        "cert digest",
        "author",
        "round",
        "epoch",
        "signers",
        "state",
        "signature",
    ]);
    for n in &r.nodes {
        match &n.leader {
            Some(c) => t.row(vec![
                n.node.clone(),
                n.live.to_string(),
                opt(&n.tier),
                c.summary.digest.clone(),
                c.summary.author.clone(),
                c.summary.round.to_string(),
                c.summary.epoch.to_string(),
                format!("{:?}", c.signers),
                c.verification_state.to_owned(),
                c.signature.clone().unwrap_or_else(|| "-".to_owned()),
            ]),
            None => {
                t.row(vec![n.node.clone(), n.live.to_string(), describe_absent(n.lookup, n.tip)])
            }
        }
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(c) = &n.leader else { continue };
        if c.parents.is_none() && c.payload.is_none() {
            continue;
        }
        let _ = writeln!(
            out,
            "\n[{}] header digest {} created_at {}",
            n.node, c.header_digest, c.created_at
        );
        if let Some(parents) = &c.parents {
            let _ = writeln!(out, "  parents:");
            for p in parents {
                let _ = writeln!(out, "    {p}");
            }
        }
        if let Some(payload) = &c.payload {
            let _ = writeln!(out, "  payload:");
            for p in payload {
                let _ = writeln!(out, "    {} worker {}", p.batch, p.worker);
            }
        }
    }
}

fn render_walk(r: &WalkReport, out: &mut String) {
    let _ = writeln!(out, "walk headers from {} back {}", r.start, r.back);
    for n in &r.nodes {
        let state = if n.ok {
            format!("ok ({})", n.stopped)
        } else if n.start_not_reached {
            format!("not reached (tip {})", opt(&n.tip))
        } else {
            format!("BROKEN ({})", n.stopped)
        };
        let _ = writeln!(out, "\n[{}] live={} {state}", n.node, n.live);
        let mut t = Table::new(&["number", "tier", "digest", "parent_hash", "leader r/e", "link"]);
        for h in &n.hops {
            t.row(vec![
                h.number.to_string(),
                h.tier.to_string(),
                h.digest.clone(),
                h.parent_hash.clone(),
                format!("{}/{}", h.leader_round, h.leader_epoch),
                h.link.to_string(),
            ]);
        }
        out.push_str(&t.render());
    }
}

fn render_summary(r: &SummaryReport, out: &mut String) {
    let _ = writeln!(out, "summary");
    let mut t = Table::new(&[
        "node",
        "live",
        "mdbx.dat",
        "epochs",
        "records",
        "certs",
        "consensus #",
        "cache #",
        "cold",
        "checkpoints",
    ]);
    for n in &r.nodes {
        t.row(vec![
            n.node.clone(),
            n.live.to_string(),
            human_bytes(n.datafile_bytes),
            match (n.first_epoch, n.last_epoch) {
                (Some(f), Some(l)) => format!("{f}..={l}"),
                _ => "none".to_owned(),
            },
            n.epoch_records.to_string(),
            n.epoch_certs.to_string(),
            opt(&n.latest_consensus_number),
            opt(&n.latest_cached_consensus_number),
            if n.cold_tier {
                format!("yes (hwm {})", opt(&n.cold_high_water_mark))
            } else {
                "no".to_owned()
            },
            if n.leftover_checkpoints.is_empty() {
                "none".to_owned()
            } else {
                format!("LEFTOVER x{}", n.leftover_checkpoints.len())
            },
        ]);
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let _ = writeln!(out, "\n[{}] {}", n.node, n.path);
        let _ = writeln!(out, "  identity  {}", opt(&n.node_identity));
        for cp in &n.leftover_checkpoints {
            let _ = writeln!(
                out,
                "  checkpoint epoch {} phase {} target {} at {}",
                cp.epoch, cp.completed_phase, cp.target_hash, cp.timestamp
            );
        }
        let mut t = Table::new(&["table", "entries"]);
        for (name, count) in &n.tables {
            t.row(vec![name.clone(), count.to_string()]);
        }
        for line in t.render().lines() {
            let _ = writeln!(out, "  {line}");
        }
    }
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_aligns_columns() {
        let mut t = Table::new(&["a", "bbb"]);
        t.row(vec!["xxxx".into(), "y".into()]);
        let s = t.render();
        assert_eq!(s, "a     bbb\n----  ---\nxxxx  y\n");
    }

    #[test]
    fn bytes_are_humanized() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(3 * 1024 * 1024), "3.0 MiB");
    }
}
