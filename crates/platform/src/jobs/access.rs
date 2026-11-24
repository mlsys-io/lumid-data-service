//! Who may use a source: the owner, anyone (read-only) when it is public, or a
//! grantee holding an active share.
//!
//! The rule is a pure function ([`decide`]) so it can be tested exhaustively;
//! [`can`] only loads the rows. It is evaluated for a human at plan/submit
//! time AND re-evaluated for a running job (as its owner) every time its scope
//! is refreshed, so revoking a share cuts off jobs that already hold a token.

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;

/// Read or write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Access {
    Read,
    Write,
}

impl Access {
    pub fn as_str(self) -> &'static str {
        match self {
            Access::Read => "read",
            Access::Write => "write",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "read" => Some(Access::Read),
            "write" => Some(Access::Write),
            _ => None,
        }
    }
}

/// The parts of a `jobs.sources` row the rule needs.
#[derive(Clone, Debug)]
pub struct SourceFacts {
    pub source_id: String,
    pub owner_sub: String,
    pub kind: String,
    pub visibility: String,
    pub deleted: bool,
}

/// The parts of a `jobs.source_grants` row the rule needs.
#[derive(Clone, Debug)]
pub struct GrantFacts {
    pub grantee_sub: String,
    pub access: Access,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked: bool,
}

/// The outcome, with a reason safe to show the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    pub allow: bool,
    pub reason: &'static str,
}

const fn allow(reason: &'static str) -> Decision {
    Decision {
        allow: true,
        reason,
    }
}
const fn deny(reason: &'static str) -> Decision {
    Decision {
        allow: false,
        reason,
    }
}

/// The access rule.
///
/// * A deleted source is nobody's.
/// * Estate sources (`owner_sub = system`) are read-only through jobs: writing
///   into an estate table still goes through `/ingest` and its own ACL.
/// * The owner may read and write.
/// * A public source may be read by anyone.
/// * Otherwise an active (unrevoked, unexpired) grant to `sub` is needed. A
///   write grant implies read.
///
/// Privileged roles get no bypass here: a job acts as its owner, and admin
/// oversight belongs to a separate, explicit path.
pub fn decide(
    sub: &str,
    src: &SourceFacts,
    access: Access,
    grants: &[GrantFacts],
    now: DateTime<Utc>,
) -> Decision {
    if src.deleted {
        return deny("source_deleted");
    }
    if src.kind == "estate" && access == Access::Write {
        return deny("estate_sources_are_read_only");
    }
    if !sub.is_empty() && src.owner_sub == sub {
        return allow("owner");
    }
    if access == Access::Read && src.visibility == "public" {
        return allow("public");
    }
    let granted = grants.iter().any(|g| {
        g.grantee_sub == sub
            && !g.revoked
            && g.expires_at.is_none_or(|t| t > now)
            && (g.access == access || (access == Access::Read && g.access == Access::Write))
    });
    if granted {
        allow("shared")
    } else {
        deny("source_not_entitled")
    }
}

/// Load the source + its grants for `sub` and apply [`decide`]. An unknown
/// source is `source_not_found` (callers should not reveal more than that).
pub async fn can(
    pool: &Pool,
    sub: &str,
    source_id: &str,
    access: Access,
) -> anyhow::Result<Decision> {
    let client = pool.get().await?;
    let Some(row) = client
        .query_opt(
            "SELECT owner_sub, kind, visibility, deleted_at IS NOT NULL AS deleted
               FROM jobs.sources WHERE source_id = $1",
            &[&source_id],
        )
        .await?
    else {
        return Ok(deny("source_not_found"));
    };
    let src = SourceFacts {
        source_id: source_id.to_string(),
        owner_sub: row.get("owner_sub"),
        kind: row.get("kind"),
        visibility: row.get("visibility"),
        deleted: row.get("deleted"),
    };
    let grants = client
        .query(
            "SELECT grantee_sub, access, expires_at, revoked_at IS NOT NULL AS revoked
               FROM jobs.source_grants WHERE source_id = $1 AND grantee_sub = $2",
            &[&source_id, &sub],
        )
        .await?
        .into_iter()
        .filter_map(|r| {
            Some(GrantFacts {
                grantee_sub: r.get("grantee_sub"),
                access: Access::parse(r.get::<_, &str>("access"))?,
                expires_at: r.get("expires_at"),
                revoked: r.get("revoked"),
            })
        })
        .collect::<Vec<_>>();
    Ok(decide(sub, &src, access, &grants, Utc::now()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn src(owner: &str, kind: &str, vis: &str) -> SourceFacts {
        SourceFacts {
            source_id: "s".into(),
            owner_sub: owner.into(),
            kind: kind.into(),
            visibility: vis.into(),
            deleted: false,
        }
    }
    fn grant(to: &str, a: Access) -> GrantFacts {
        GrantFacts {
            grantee_sub: to.into(),
            access: a,
            expires_at: None,
            revoked: false,
        }
    }
    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn owner_reads_and_writes_their_private_source() {
        let s = src("alice", "table", "private");
        assert_eq!(
            decide("alice", &s, Access::Read, &[], now()),
            allow("owner")
        );
        assert_eq!(
            decide("alice", &s, Access::Write, &[], now()),
            allow("owner")
        );
    }

    #[test]
    fn strangers_get_nothing_from_a_private_source() {
        let s = src("alice", "table", "private");
        assert!(!decide("bob", &s, Access::Read, &[], now()).allow);
        assert!(!decide("bob", &s, Access::Write, &[], now()).allow);
        // A grant to someone else does not help.
        assert!(
            !decide(
                "bob",
                &s,
                Access::Read,
                &[grant("carol", Access::Read)],
                now()
            )
            .allow
        );
        // An empty sub never matches an owner.
        assert!(!decide("", &src("", "table", "private"), Access::Read, &[], now()).allow);
    }

    #[test]
    fn public_is_read_only_for_non_owners() {
        let s = src("alice", "query", "public");
        assert_eq!(decide("bob", &s, Access::Read, &[], now()), allow("public"));
        assert!(!decide("bob", &s, Access::Write, &[], now()).allow);
    }

    #[test]
    fn grants_share_read_and_write_implies_read() {
        let s = src("alice", "blob_set", "shared");
        assert_eq!(
            decide(
                "bob",
                &s,
                Access::Read,
                &[grant("bob", Access::Read)],
                now()
            ),
            allow("shared")
        );
        assert!(
            !decide(
                "bob",
                &s,
                Access::Write,
                &[grant("bob", Access::Read)],
                now()
            )
            .allow
        );
        assert!(
            decide(
                "bob",
                &s,
                Access::Read,
                &[grant("bob", Access::Write)],
                now()
            )
            .allow
        );
        assert!(
            decide(
                "bob",
                &s,
                Access::Write,
                &[grant("bob", Access::Write)],
                now()
            )
            .allow
        );
    }

    #[test]
    fn revoked_and_expired_grants_do_nothing() {
        let s = src("alice", "table", "shared");
        let mut g = grant("bob", Access::Read);
        g.revoked = true;
        assert!(!decide("bob", &s, Access::Read, &[g], now()).allow);
        let mut g = grant("bob", Access::Read);
        g.expires_at = Some(now() - Duration::seconds(1));
        assert!(!decide("bob", &s, Access::Read, &[g], now()).allow);
        let mut g = grant("bob", Access::Read);
        g.expires_at = Some(now() + Duration::hours(1));
        assert!(decide("bob", &s, Access::Read, &[g], now()).allow);
    }

    #[test]
    fn deleted_sources_are_nobodys_even_the_owners() {
        let mut s = src("alice", "table", "public");
        s.deleted = true;
        assert_eq!(
            decide("alice", &s, Access::Read, &[], now()),
            deny("source_deleted")
        );
        assert!(
            !decide(
                "bob",
                &s,
                Access::Read,
                &[grant("bob", Access::Read)],
                now()
            )
            .allow
        );
    }

    #[test]
    fn estate_sources_are_public_read_and_never_writable_through_jobs() {
        let s = src("system", "estate", "public");
        assert!(decide("bob", &s, Access::Read, &[], now()).allow);
        assert_eq!(
            decide(
                "bob",
                &s,
                Access::Write,
                &[grant("bob", Access::Write)],
                now()
            ),
            deny("estate_sources_are_read_only")
        );
        assert!(!decide("system", &s, Access::Write, &[], now()).allow);
    }
}
