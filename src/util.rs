//! Fungsi murni (tanpa dependensi) supaya mudah diuji.

use std::time::Duration;

pub const BADGE_TIERS: [(f64, &str); 10] = [
    (1.0, "Tier 1: Newcomer (1 hour)"),
    (5.0, "Tier 2: Fledgling (5 hours)"),
    (20.0, "Tier 3: Breakout (20 hours)"),
    (75.0, "Tier 4: Standout (75 hours)"),
    (150.0, "Tier 5: Trendsetter (150 hours)"),
    (300.0, "Tier 6: Headliner (300 hours)"),
    (500.0, "Tier 7: Star (500 hours)"),
    (1000.0, "Tier 8: Sensation (1,000 hours)"),
    (2000.0, "Tier 9: Visionary (2,000 hours)"),
    (5000.0, "Tier 10: Phenomenon (5,000+ hours)"),
];

/// Hasil klasifikasi teks "room" dari pengguna (belum menghubungi Discord).
#[derive(Debug, PartialEq)]
pub enum RoomRef {
    /// Link https://discord.com/channels/GUILD_ID/CHANNEL_ID (GUILD bisa @me)
    Url {
        guild_id: Option<String>,
        channel_id: String,
    },
    /// "GUILD_ID:CHANNEL_ID"
    Pair {
        guild_id: String,
        channel_id: String,
    },
    /// Hanya CHANNEL_ID
    Single(String),
}

pub fn is_id(s: &str) -> bool {
    (15..=22).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit())
}

pub fn digit_runs(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (i, c) in text.char_indices() {
        if c.is_ascii_digit() {
            if start.is_none() {
                start = Some(i);
            }
        } else if let Some(s) = start.take() {
            out.push(&text[s..i]);
        }
    }
    if let Some(s) = start {
        out.push(&text[s..]);
    }
    out
}

pub fn classify_room(raw: &str) -> Result<RoomRef, String> {
    let text = raw.trim();

    if let Some(pos) = text.find("/channels/") {
        let rest = &text[pos + "/channels/".len()..];
        let mut parts = rest.split('/');
        let g = parts.next().unwrap_or("");
        let c: String = parts
            .next()
            .unwrap_or("")
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect();
        let g_ok = g == "@me" || (!g.is_empty() && g.bytes().all(|b| b.is_ascii_digit()));
        if g_ok && !c.is_empty() {
            return Ok(RoomRef::Url {
                guild_id: if g == "@me" {
                    None
                } else {
                    Some(g.to_string())
                },
                channel_id: c,
            });
        }
    }

    let runs = digit_runs(text);
    let only_sep = text
        .chars()
        .all(|c| c.is_ascii_digit() || c == ':' || c == '/' || c.is_whitespace());
    if runs.len() == 2 && only_sep && runs.iter().all(|r| is_id(r)) {
        return Ok(RoomRef::Pair {
            guild_id: runs[0].to_string(),
            channel_id: runs[1].to_string(),
        });
    }

    match runs.iter().find(|r| is_id(r)) {
        Some(id) => Ok(RoomRef::Single((*id).to_string())),
        None => Err("Invalid room ID or URL. Gunakan Channel ID atau link:\n\
                     https://discord.com/channels/GUILD_ID/CHANNEL_ID"
            .to_string()),
    }
}

pub fn format_duration(secs: u64) -> String {
    let (h, rem) = (secs / 3600, secs % 3600);
    format!("{:02}h:{:02}m:{:02}s", h, rem / 60, rem % 60)
}

pub fn next_badge(hours: f64) -> String {
    for (req, title) in BADGE_TIERS.iter() {
        if hours < *req {
            return format!("{} ({:.1}h remaining)", title, req - hours);
        }
    }
    "Tier 10: Phenomenon (max badge reached!)".to_string()
}

/// Jeda rejoin: 3s, 6s, 12s, ... dibatasi `max`.
pub fn rejoin_delay(count: u32, max: Duration) -> Duration {
    Duration::from_secs(3)
        .saturating_mul(2u32.saturating_pow(count.min(10)))
        .min(max)
}

#[cfg(test)]
mod tests {
    use super::*;

    const G: &str = "123456789012345678";
    const C: &str = "223456789012345678";

    #[test]
    fn url_with_guild() {
        let r = classify_room(&format!("https://discord.com/channels/{G}/{C}")).unwrap();
        assert_eq!(
            r,
            RoomRef::Url {
                guild_id: Some(G.into()),
                channel_id: C.into()
            }
        );
    }

    #[test]
    fn url_dm_call_with_query() {
        let r = classify_room(&format!("https://discord.com/channels/@me/{C}?x=1")).unwrap();
        assert_eq!(
            r,
            RoomRef::Url {
                guild_id: None,
                channel_id: C.into()
            }
        );
    }

    #[test]
    fn pair_and_single() {
        assert_eq!(
            classify_room(&format!("{G}:{C}")).unwrap(),
            RoomRef::Pair {
                guild_id: G.into(),
                channel_id: C.into()
            }
        );
        assert_eq!(classify_room(C).unwrap(), RoomRef::Single(C.into()));
    }

    #[test]
    fn invalid_input() {
        assert!(classify_room("hello").is_err());
        assert!(classify_room("12345").is_err());
    }

    #[test]
    fn duration_and_badge() {
        assert_eq!(format_duration(3725), "01h:02m:05s");
        assert!(next_badge(0.5).starts_with("Tier 1:"));
        assert!(next_badge(6000.0).contains("max badge"));
    }

    #[test]
    fn rejoin_backoff() {
        let max = Duration::from_secs(120);
        assert_eq!(rejoin_delay(0, max), Duration::from_secs(3));
        assert_eq!(rejoin_delay(1, max), Duration::from_secs(6));
        assert_eq!(rejoin_delay(2, max), Duration::from_secs(12));
        assert_eq!(rejoin_delay(10, max), max);
        assert_eq!(rejoin_delay(99, max), max);
    }
}
