// Copyright 2020, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::Result;
use crate::test::stats;
use crate::test::tag::TagFilter;
use std::io::Write;
use termcolor::{Color, ColorSpec};
use tremor_script::highlighter::{Highlighter, Term as TermHighlighter};

macro_rules! fg_bold {
    ($h:ident, $c:ident) => {
        $h.set_color(ColorSpec::new().set_fg(Some(Color::$c)).set_bold(true))?;
    };
}

pub(crate) fn h0(label: &str, what: &str) -> Result<()> {
    let mut h = TermHighlighter::new();
    fg_bold!(h, White);
    write!(h.get_writer(), "{}", &label)?;
    h.reset()?;
    writeln!(h.get_writer(), ": {}", &what.trim())?;
    h.reset()?;
    h.finalize()?;
    drop(h);
    Ok(())
}

pub(crate) fn h1(label: &str, what: &str) -> Result<()> {
    let mut h = TermHighlighter::new();
    fg_bold!(h, White);
    write!(h.get_writer(), "  {}", &label)?;
    h.reset()?;
    writeln!(h.get_writer(), ": {}", &what.trim())?;
    h.reset()?;
    h.finalize()?;
    drop(h);
    Ok(())
}

fn humanize(ts: u64) -> String {
    let mut ns = ts;
    if ns > 999 {
        ns = ts % 1000;
        let us = (ts / 1000) as u64;
        if us > 999 {
            let ms = (ts / 1_000_000) as u64;
            if ms > 999 {
                let secs = (ts / 1_000_000_000) as u64;
                if secs > 59 {
                    let mins = (ts / 60_000_000_000) as u64;
                    if mins > 59 {
                        let hrs = (ts / 3_600_000_000_000) as u64;
                        return format!("{}h {}m {:02}s", hrs, mins % 60, secs % 60);
                    } else {
                        return format!("{}m {}s", mins, secs % 60);
                    }
                } else {
                    return format!("{}s {}ms", secs, ms % 1000);
                }
            } else {
                return format!("{}ms {}us", ms, us % 1000);
            }
        } else {
            return format!("{}us {}ns", us, ns % 1000);
        }
    } else {
        return format!("{}ns", ns);
    }
}

pub(crate) fn duration(what: u64, prefix: &str) -> Result<()> {
    let mut h = TermHighlighter::new();
    fg_bold!(h, Blue);
    write!(h.get_writer(), "{}Elapsed", prefix)?;
    h.reset()?;
    writeln!(h.get_writer(), ": {}", &humanize(what))?;
    h.reset()?;
    h.finalize()?;
    drop(h);
    Ok(())
}

pub(crate) fn total_duration(what: u64) -> Result<()> {
    duration(what, "Total ")
}

pub(crate) fn assert_has(
    prefix: &str,
    label: &str,
    what: &str,
    info: Option<&String>,
    ok: bool,
) -> Result<()> {
    let mut h = TermHighlighter::new();
    if ok {
        fg_bold!(h, Green);
        write!(h.get_writer(), "{}    (+) ", prefix)?;
    } else {
        write!(h.get_writer(), "{}    (-) ", prefix)?;
        fg_bold!(h, Red);
        write!(h.get_writer(), "    (-) ")?;
    }
    write!(h.get_writer(), "{}", &label)?;
    h.reset()?;
    write!(h.get_writer(), ": ")?;
    writeln!(h.get_writer(), "{}", &what)?;
    h.reset()?;
    if let Some(info) = info {
        if !ok {
            writeln!(h.get_writer(), "{}", info)?;
        }
    };
    h.finalize()?;
    drop(h);
    Ok(())
}

pub(crate) fn executing_unit_testcase(i: usize, n: usize, success: bool) -> Result<()> {
    let mut h = TermHighlighter::new();
    fg_bold!(h, Green);
    let prefix = if success {
        fg_bold!(h, Green);
        "   (+)"
    } else {
        fg_bold!(h, Red);
        "   (-)"
    };
    writeln!(
        h.get_writer(),
        "     {} Executing test {} of {}",
        prefix,
        i + 1,
        n
    )?;
    h.reset()?;
    h.finalize()?;
    drop(h);
    Ok(())
}

pub(crate) fn stats(stats: &stats::Stats, prefix: &str) -> Result<()> {
    if stats.is_zero() {
        return Ok(());
    }

    let mut h = TermHighlighter::new();
    fg_bold!(h, Blue);
    write!(h.get_writer(), "{} Stats: ", prefix)?;
    fg_bold!(h, Green);
    write!(h.get_writer(), "Pass ")?;
    write!(h.get_writer(), "{} ", stats.pass)?;
    if stats.fail > 0 {
        fg_bold!(h, Red);
    } else {
        fg_bold!(h, Black);
    }
    write!(h.get_writer(), "Fail ")?;
    write!(h.get_writer(), "{} ", stats.fail)?;
    if stats.skip > 0 {
        fg_bold!(h, Yellow);
    } else {
        fg_bold!(h, Black);
    }
    write!(h.get_writer(), "Skip ")?;
    write!(h.get_writer(), "{} ", stats.skip)?;
    fg_bold!(h, Yellow);
    write!(h.get_writer(), " Asserts ")?;
    writeln!(h.get_writer(), "{} ", stats.assert)?;
    h.reset()?;
    h.finalize()?;
    drop(h);
    Ok(())
}

pub(crate) fn hr() -> Result<()> {
    println!();
    Ok(())
}

pub(crate) fn rollups(label: &str, stats: &stats::Stats) -> Result<()> {
    if stats.is_zero() {
        return Ok(());
    }

    let mut h = TermHighlighter::new();
    fg_bold!(h, Blue);
    write!(h.get_writer(), "{} Stats: ", label)?;
    fg_bold!(h, Green);
    write!(h.get_writer(), "Pass ")?;
    write!(h.get_writer(), "{} ", stats.pass)?;
    if stats.fail > 0 {
        fg_bold!(h, Red);
    } else {
        fg_bold!(h, Black);
    }
    write!(h.get_writer(), "Fail ")?;
    write!(h.get_writer(), "{} ", stats.fail)?;
    if stats.skip > 0 {
        fg_bold!(h, Yellow);
    } else {
        fg_bold!(h, Black);
    }
    write!(h.get_writer(), "Skip ")?;
    write!(h.get_writer(), "{} ", stats.skip)?;
    fg_bold!(h, Yellow);
    write!(h.get_writer(), " Asserts ")?;
    writeln!(h.get_writer(), "{} ", stats.assert)?;
    h.reset()?;
    h.finalize()?;
    drop(h);
    Ok(())
}

pub(crate) fn tags(
    filter: &TagFilter,
    allowing: Option<&[String]>,
    denying: Option<&[String]>,
) -> Result<()> {
    tagsx("    ", filter, allowing, denying)
}

pub(crate) fn tagsx(
    prefix: &str,
    filter: &TagFilter,
    allowing: Option<&[String]>,
    denying: Option<&[String]>,
) -> Result<()> {
    const EMPTY: &[String] = &[];
    let config = match (allowing, denying) {
        (Some(allowing), Some(denying)) => (allowing, denying),
        (Some(allowing), None) => (allowing, EMPTY),
        (None, Some(denying)) => (EMPTY, denying),
        (None, None) => (EMPTY, EMPTY),
    };

    //    if let (Some(allowing), Some(denying)) = (allowing,denying) {
    let (active, _status) = filter.matches(&config.0, &config.1);
    let mut h = TermHighlighter::new();
    fg_bold!(h, Yellow);
    write!(h.get_writer(), "{}Tags: ", prefix)?;
    for tag in filter.includes() {
        if config.1.contains(&tag) {
            fg_bold!(h, Red);
        } else if active.contains(&tag) {
            fg_bold!(h, Green);
        } else {
            fg_bold!(h, Black);
        }
        write!(h.get_writer(), " {}", tag)?;
    }
    writeln!(h.get_writer())?;
    h.finalize()?;
    drop(h);
    Ok(())
}
