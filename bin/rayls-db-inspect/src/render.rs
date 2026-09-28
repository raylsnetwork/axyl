// SPDX-License-Identifier: BUSL-1.1
//! Plain-text rendering: aligned columns, one table per report, one-line verdict last.

use crate::{
    node_db::Tier,
    report::{
        batch::{describe_absent_batch, BatchReport, CommitPath, TxReport},
        epoch::{describe_position, EpochCheckReport, EpochReport, EpochStatus, EpochsReport},
        header::{describe_absent, CertReport, HeaderCheckReport, HeaderReport, SignatureCheck},
        summary::SummaryReport,
        Report,
    },
    view::{authority, b256, cert_digest, CertificateSummary, TransactionView},
};
use std::fmt::Write as _;

/// Minimal aligned-column table.
///
/// Borderless on purpose: two spaces between columns, a rule under the header, nothing else. The
/// output is meant to be piped, grepped and diffed, so there is no styling and no line that
/// depends on the terminal's width.
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
        let mut table = comfy_table::Table::new();
        table
            .load_preset(comfy_table::presets::NOTHING)
            .set_style(comfy_table::TableComponent::HeaderLines, '-')
            // Size columns to their contents. The alternative consults the terminal width, which
            // would make the output change between a pipe and a tty.
            .set_content_arrangement(comfy_table::ContentArrangement::Disabled)
            .set_header(self.headers.clone());
        for row in &self.rows {
            table.add_row(row.clone());
        }
        // A cell is padded one space either side, which is what puts two spaces between columns.
        // Drop the padding at both ends so rows start in column zero and the rule ends with them.
        let last = table.column_count().saturating_sub(1);
        for (i, column) in table.column_iter_mut().enumerate() {
            column.set_padding((u16::from(i != 0), u16::from(i != last)));
        }
        let mut out = String::new();
        for line in table.to_string().lines() {
            // Short rows are padded out to the column width; nothing downstream wants the spaces.
            let _ = writeln!(out, "{}", line.trim_end());
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

/// One word for the `verify` column.
fn verify_word(check: Option<&SignatureCheck>) -> String {
    check.map(|c| c.word()).unwrap_or("-").to_owned()
}

/// Renders `report` as text.
pub fn render(report: &Report) -> String {
    let mut out = String::new();
    match report {
        Report::Epoch(r) => render_epoch(r, &mut out),
        Report::Epochs(r) => render_epochs(r, &mut out),
        Report::EpochCheck(r) => render_epoch_check(r, &mut out),
        Report::Header(r) => render_header(r, &mut out),
        Report::Cert(r) => render_cert(r, &mut out),
        Report::GetBatch(r) => render_batch(r, &mut out),
        Report::GetTx(r) => render_tx(r, &mut out),
        Report::HeaderCheck(r) => render_header_check(r, &mut out),
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
            rec.map(|rec| match (rec.parent_consensus_number, rec.parent_consensus_tier) {
                (Some(n), Some(tier)) => format!("{n} ({tier})"),
                (Some(n), None) => format!("{n} (indexed, header missing)"),
                // the genesis record's boundary is the zero hash by construction
                (None, _) if r.epoch == 0 => "n/a (genesis)".to_owned(),
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
            let _ = writeln!(out, "  cert signers      {}", list(&c.signers));
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
    headers.extend(r.nodes.iter().map(|n| n.node.as_str()));
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

fn render_epoch_check(r: &EpochCheckReport, out: &mut String) {
    let _ = writeln!(out, "epoch check");
    let mut t = Table::new(&[
        "node",
        "range",
        "checked",
        "certified",
        "gaps",
        "broken links",
        "uncertified",
        "invalid certs",
        "handoff mismatch",
        "index mismatch",
        "ok",
    ]);
    for n in &r.nodes {
        t.row(vec![
            n.node.clone(),
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
            list(&n.index_mismatch),
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
        let Some(records) = &n.records else { continue };
        let _ = writeln!(out, "\n[{}]", n.node);
        let mut t = Table::new(&[
            "epoch",
            "digest",
            "parent_hash",
            "cert",
            "index",
            "link",
            "handoff",
            "committee",
        ]);
        for rec in records {
            t.row(vec![
                rec.epoch.to_string(),
                rec.digest.clone(),
                rec.parent_hash.clone(),
                rec.cert.to_owned(),
                yes_no(rec.index_ok).to_owned(),
                rec.link.to_string(),
                opt_yes_no(rec.committee_handoff_ok),
                rec.committee_size.to_string(),
            ]);
        }
        for line in t.render().lines() {
            let _ = writeln!(out, "  {line}");
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
        "tier",
        "digest",
        "parent_hash",
        "leader round",
        "leader epoch",
        "certs",
        "batches",
        "commit ts",
        "verify",
    ]);
    for n in &r.nodes {
        match &n.header {
            Some(h) => t.row(vec![
                n.node.clone(),
                opt(&n.tier),
                h.digest.clone(),
                h.parent_hash.clone(),
                h.leader.round.to_string(),
                h.leader.epoch.to_string(),
                h.certificate_count.to_string(),
                h.batch_count.to_string(),
                h.commit_timestamp.to_string(),
                verify_word(n.signature_check.as_ref()),
            ]),
            None => t.row(vec![n.node.clone(), describe_absent(n.lookup, n.tip)]),
        }
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(h) = &n.header else { continue };
        let _ = writeln!(out, "\n[{}]", n.node);
        if let Some(check) = &n.signature_check {
            let _ = writeln!(out, "  verify        {check}");
        }
        let _ = writeln!(out, "  leader        {} by {}", h.leader.digest, h.leader.author);
        let _ = writeln!(out, "  extra         {}", h.extra);
        let _ =
            writeln!(out, "  reputation    final_of_schedule={}", h.reputation_final_of_schedule);
        if let Some(raw) = &h.raw {
            let _ = writeln!(out, "  sub-dag certificates:");
            if raw.sub_dag.certificates.is_empty() {
                let _ = writeln!(out, "    none");
            }
            for c in &raw.sub_dag.certificates {
                let s = CertificateSummary::of(c);
                let _ = writeln!(
                    out,
                    "    {} r{} e{} by {} signers={}",
                    s.digest,
                    s.round,
                    s.epoch,
                    s.author,
                    c.signed_authorities().len()
                );
            }
        }
        if let Some(batches) = &h.batches {
            if batches.is_empty() {
                let _ = writeln!(out, "  batches       none");
            } else {
                let _ = writeln!(out, "  batches:");
            }
            for b in batches {
                let state = match (b.tier, b.dangling) {
                    (Some(tier), _) => tier.to_string(),
                    (None, true) => "DANGLING (cold index, no jar row)".to_owned(),
                    // a cached header has not been executed, so its batches need not be here yet
                    (None, false) if n.tier == Some(Tier::Cache) => {
                        "not held (header not processed yet)".to_owned()
                    }
                    (None, false) => "MISSING".to_owned(),
                };
                let _ = writeln!(out, "    {} {state}", b.digest);
            }
        }
        if let Some(raw) = &h.raw {
            let _ = writeln!(out, "  reputation scores:");
            if raw.sub_dag.reputation_score.scores_per_authority.is_empty() {
                let _ = writeln!(out, "    none");
            }
            for (id, score) in raw.sub_dag.reputation_score.authorities_by_score_desc() {
                let _ = writeln!(out, "    {} {score}", authority(&id));
            }
        }
    }
}

fn render_cert(r: &CertReport, out: &mut String) {
    let _ = writeln!(out, "leader cert of header {}", r.number);
    let mut t = Table::new(&[
        "node",
        "tier",
        "header digest",
        "cert digest",
        "author",
        "round",
        "epoch",
        "signers",
        "state",
        "signature",
        "verify",
    ]);
    for n in &r.nodes {
        match &n.leader {
            Some(c) => t.row(vec![
                n.node.clone(),
                opt(&n.tier),
                n.header_digest.clone().unwrap_or_else(|| "-".to_owned()),
                c.summary.digest.clone(),
                c.summary.author.clone(),
                c.summary.round.to_string(),
                c.summary.epoch.to_string(),
                list(&c.signers),
                c.verification_state.to_owned(),
                c.signature.clone().unwrap_or_else(|| "-".to_owned()),
                verify_word(n.signature_check.as_ref()),
            ]),
            None => t.row(vec![n.node.clone(), describe_absent(n.lookup, n.tip)]),
        }
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(c) = &n.leader else { continue };
        if let Some(check) = &n.signature_check {
            let _ = writeln!(out, "\n[{}] verify {check}", n.node);
        }
        let Some(raw) = &c.raw else { continue };
        let _ = writeln!(
            out,
            "
[{}] leader header digest {} created_at {}",
            n.node, c.header_digest, c.created_at
        );
        let _ = writeln!(out, "  parents:");
        if raw.header().parents().is_empty() {
            let _ = writeln!(out, "    none");
        }
        for p in raw.header().parents() {
            let _ = writeln!(out, "    {}", cert_digest(*p));
        }
        let _ = writeln!(out, "  payload:");
        if raw.header().payload().is_empty() {
            let _ = writeln!(out, "    none");
        }
        for (batch, worker) in raw.header().payload() {
            let _ = writeln!(out, "    {} worker {worker}", b256(batch));
        }
    }
}

fn render_batch(r: &BatchReport, out: &mut String) {
    let _ = writeln!(out, "batch {}", r.digest);
    let mut t = Table::new(&[
        "node",
        "tier",
        "epoch",
        "worker",
        "seq",
        "txs",
        "bytes",
        "base fee",
        "digest ok",
        "round",
        "committed in",
    ]);
    for n in &r.nodes {
        match (&n.batch, &n.dangling) {
            (Some(b), _) => t.row(vec![
                n.node.clone(),
                opt(&n.tier),
                b.epoch.to_string(),
                b.worker_id.to_string(),
                b.seq.to_string(),
                b.transaction_count.to_string(),
                b.transaction_bytes.to_string(),
                b.base_fee_per_gas.to_string(),
                yes_no(b.digest_ok).to_owned(),
                carrier_round(b.path.as_ref()),
                b.committed_in.to_string(),
            ]),
            (None, Some(loc)) => t.row(vec![
                n.node.clone(),
                format!("DANGLING (cold index epoch {} row {}, no jar row)", loc.epoch, loc.row),
            ]),
            (None, None) => {
                t.row(vec![n.node.clone(), describe_absent_batch(n.lookup, n.tip, r.committed_at)])
            }
        }
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(b) = &n.batch else { continue };
        let _ = writeln!(out, "\n[{}]", n.node);
        if !b.digest_ok {
            section(out, "stored bytes hash to", &b.computed_digest);
        }
        section(out, "authority", &b.authority);
        section(out, "committed in", b.committed_in);
        if let Some(path) = &b.path {
            render_commit_path(path, out);
        }
        if b.transactions.is_empty() {
            section(out, "transactions", "none");
        } else {
            section(out, "transactions", b.transactions.len());
            for tx in &b.transactions {
                render_transaction_entry(tx, out);
            }
        }
    }
}

/// A detail-block heading: two-space indent, the key padded so values line up.
fn section(out: &mut String, key: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "  {key:<13} {value}");
}

/// A field under a [`section`]: four-space indent, the key padded so values line up. One value
/// per line, so a 32-byte hash never shares a line with another.
fn field(out: &mut String, key: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "    {key:<11} {value}");
}

/// One transaction of a batch listing: its position and hash, then the decoded fields. Four
/// lines, none carrying more than one address or hash.
fn render_transaction_entry(tx: &TransactionView, out: &mut String) {
    let _ = writeln!(out, "    [{}] {}", tx.index, tx.hash);
    if let Some(err) = &tx.error {
        let _ = writeln!(out, "        {} bytes UNDECODABLE: {err}", tx.bytes);
        return;
    }
    let _ = writeln!(out, "        from {}", tx.from.as_deref().unwrap_or("UNRECOVERABLE"));
    let _ = writeln!(out, "        to   {}", opt(&tx.to));
    let _ = writeln!(
        out,
        "        type {} nonce {} value {} gas {} {} bytes",
        opt(&tx.tx_type),
        opt(&tx.nonce),
        opt(&tx.value),
        opt(&tx.gas_limit),
        tx.bytes
    );
}

/// The DAG round of the certificate that carried the batch into its commit; `-` when nothing
/// committed it or no certificate of the sub-dag lists it.
fn carrier_round(path: Option<&CommitPath>) -> String {
    path.and_then(|p| p.certificate.as_ref())
        .map(|c| c.certificate.summary.round.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

/// The stored rows between a batch and its commit: the sub-dag certificate whose payload lists
/// the batch, then the consensus header carrying that sub-dag.
fn render_commit_path(path: &CommitPath, out: &mut String) {
    match &path.certificate {
        Some(c) => {
            let s = &c.certificate.summary;
            section(out, "certificate", &s.digest);
            field(out, "author", &s.author);
            field(out, "round", s.round);
            field(out, "epoch", s.epoch);
            field(out, "worker", c.worker_id);
            field(out, "header", &c.certificate.header_digest);
            field(out, "created at", c.certificate.created_at);
            field(out, "signers", list(&c.certificate.signers));
            field(out, "state", c.certificate.verification_state);
        }
        None => section(out, "certificate", "NONE of the sub-dag lists this batch"),
    }
    let h = &path.header;
    section(out, "header", h.number);
    field(out, "digest", &h.digest);
    field(out, "parent", &h.parent_hash);
    field(out, "leader", &h.leader.digest);
    field(out, "author", &h.leader.author);
    field(out, "round", h.leader.round);
    field(out, "epoch", h.leader.epoch);
    field(out, "certs", h.certificate_count);
    field(out, "batches", h.batch_count);
    field(out, "committed", h.commit_timestamp);
}

fn render_tx(r: &TxReport, out: &mut String) {
    match r.epoch {
        Some(e) => {
            let _ = writeln!(out, "transaction {} (batches of epoch {e})", r.hash);
        }
        None => {
            let _ = writeln!(out, "transaction {}", r.hash);
        }
    }
    let mut t = Table::new(&[
        "node",
        "tier",
        "index",
        "epoch",
        "worker",
        "seq",
        "digest ok",
        "round",
        "committed in",
        "scanned",
    ]);
    for n in &r.nodes {
        if n.matches.is_empty() {
            let absence = if n.skipped {
                format!("not reached (node in epoch {}), not scanned", opt(&n.current_epoch))
            } else {
                format!(
                    "{}; scanned {}",
                    describe_absent_batch(n.lookup, n.tip, r.committed_at),
                    n.scanned
                )
            };
            t.row(vec![n.node.clone(), absence]);
            continue;
        }
        // one row per batch that carries the transaction, in the order of the detail blocks
        // below (which carry the digests); the scan total once per node
        for (i, m) in n.matches.iter().enumerate() {
            t.row(vec![
                n.node.clone(),
                m.tier.to_string(),
                format!("{}/{}", m.index, m.transaction_count),
                m.epoch.to_string(),
                m.worker_id.to_string(),
                m.seq.to_string(),
                yes_no(m.digest_ok).to_owned(),
                carrier_round(m.path.as_ref()),
                m.committed_in.to_string(),
                if i == 0 { n.scanned.to_string() } else { String::new() },
            ]);
        }
    }
    out.push_str(&t.render());
    for n in &r.nodes {
        let Some(tx) = &n.transaction else { continue };
        let _ = writeln!(out, "\n[{}]", n.node);
        render_transaction(tx, out);
        // the stored rows from the transaction up to its commit, once per batch holding it
        for m in &n.matches {
            section(out, "batch", &m.digest);
            field(out, "tier", m.tier);
            field(out, "index", format!("{}/{}", m.index, m.transaction_count));
            field(out, "epoch", m.epoch);
            field(out, "worker", m.worker_id);
            field(out, "seq", m.seq);
            field(out, "authority", &m.authority);
            field(out, "committed", m.committed_in);
            if let Some(path) = &m.path {
                render_commit_path(path, out);
            }
        }
    }
}

fn render_transaction(tx: &TransactionView, out: &mut String) {
    let _ = writeln!(out, "  hash      {}", tx.hash);
    let _ = writeln!(out, "  bytes     {}", tx.bytes);
    if let Some(err) = &tx.error {
        let _ = writeln!(out, "  UNDECODABLE: {err}");
        return;
    }
    let _ = writeln!(out, "  type      {}", opt(&tx.tx_type));
    let _ = writeln!(out, "  chain id  {}", opt(&tx.chain_id));
    let _ = writeln!(out, "  nonce     {}", opt(&tx.nonce));
    let _ = writeln!(out, "  from      {}", tx.from.as_deref().unwrap_or("UNRECOVERABLE"));
    let _ = writeln!(out, "  to        {}", opt(&tx.to));
    let _ = writeln!(out, "  value     {}", opt(&tx.value));
    let _ = writeln!(out, "  gas limit {}", opt(&tx.gas_limit));
    let _ = writeln!(out, "  max fee   {}", opt(&tx.max_fee_per_gas));
}

fn render_header_check(r: &HeaderCheckReport, out: &mut String) {
    let _ = writeln!(out, "header check from {} back {}", r.start, r.back);
    for n in &r.nodes {
        let unverifiable: usize = n.unverifiable.iter().map(|u| u.hops).sum();
        let mut state = if let Some(err) = &n.error {
            format!("UNREADABLE ({err})")
        } else if n.ok {
            format!("ok ({})", n.stopped)
        } else if n.start_not_reached {
            format!("not reached (tip {})", opt(&n.tip))
        } else if n.start_missing {
            format!("MISSING ({})", n.stopped)
        } else {
            format!("BROKEN ({})", n.stopped)
        };
        if unverifiable > 0 {
            state.push_str(&format!("; {unverifiable} unverifiable"));
        }
        let _ = writeln!(
            out,
            "
[{}] {state}",
            n.node
        );
        if n.hops.is_empty() {
            continue;
        }
        let mut t = Table::new(&[
            "number",
            "tier",
            "digest",
            "parent_hash",
            "leader r/e",
            "link",
            "verify",
        ]);
        for h in &n.hops {
            t.row(vec![
                h.number.to_string(),
                h.tier.to_string(),
                h.digest.clone(),
                h.parent_hash.clone(),
                format!("{}/{}", h.leader_round, h.leader_epoch),
                h.link.to_string(),
                h.verify.word().to_owned(),
            ]);
        }
        out.push_str(&t.render());
        // the column says ok/genesis/FAILED/no keys; spell out anything that is not ok, with the
        // unverifiable hops summed per epoch (they come in whole epochs)
        for h in n.hops.iter().filter(|h| {
            !matches!(h.verify, SignatureCheck::Verified { .. } | SignatureCheck::NoKeys { .. })
        }) {
            let _ = writeln!(out, "  header {} verify {}", h.number, h.verify);
        }
        for u in &n.unverifiable {
            let records =
                u.missing_records.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(" or ");
            let _ = writeln!(
                out,
                "  {} header{} of epoch {} cannot be verified: this node holds no record for epoch \
                 {records}; check its epoch records (epoch-check)",
                u.hops,
                if u.hops == 1 { "" } else { "s" },
                u.epoch
            );
        }
    }
}

/// `YYYY-MM-DD HH:MM:SS UTC` for unix seconds (proleptic Gregorian, days-from-civil inverse).
pub fn utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02} UTC", rem / 3600, rem % 3600 / 60, rem % 60)
}

fn render_summary(r: &SummaryReport, out: &mut String) {
    let _ = writeln!(out, "summary");
    let mut t = Table::new(&[
        "node",
        "recovered",
        "mdbx.dat",
        "epochs",
        "records",
        "certs",
        "consensus #",
        "tip at",
        "cache tip",
        "cold",
        "checkpoints",
    ]);
    for n in &r.nodes {
        t.row(vec![
            n.node.clone(),
            yes_no(n.recovered).to_owned(),
            human_bytes(n.datafile_bytes),
            match (n.first_epoch, n.last_epoch) {
                (Some(f), Some(l)) => format!("{f}..={l}"),
                _ => "none".to_owned(),
            },
            n.epoch_records.to_string(),
            n.epoch_certs.to_string(),
            opt(&n.latest_consensus_number),
            n.latest_consensus_timestamp.map_or_else(|| "-".to_owned(), utc),
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
        assert_eq!(s, "a     bbb\n---------\nxxxx  y\n");
    }

    /// A row shorter than the header is how an absent record is reported; the missing cells must
    /// not push the row out of line or leave padding behind.
    #[test]
    fn short_rows_keep_their_columns() {
        let mut t = Table::new(&["a", "bbb", "cc"]);
        t.row(vec!["xxxx".into(), "y".into(), "zz".into()]);
        t.row(vec!["w".into()]);
        let s = t.render();
        assert_eq!(s, "a     bbb  cc\n-------------\nxxxx  y    zz\nw\n");
    }

    #[test]
    fn bytes_are_humanized() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(3 * 1024 * 1024), "3.0 MiB");
    }
}
