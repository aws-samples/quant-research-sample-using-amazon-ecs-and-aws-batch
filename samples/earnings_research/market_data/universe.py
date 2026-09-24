"""Single source of truth for the FactSet US common-equity universe.

Reporters (event_source), peers (peer_deriver), and reconcile ALL apply the SAME
predicate: fix the universe in ONE place, it fixes everywhere.

Universe predicate (ONE rule, two AND'd conditions):
  - primary listing venue  sym_coverage.fref_listing_exchange in US venues
  - common stock, single class
        regional security type == primary security type == 'SHARE'

Why this rule (empirically verified against the datashare):
  - NO domicile filter. A foreign-domiciled company that CHOOSES to list its
    common stock on a US exchange is in-universe on the same basis as everyone
    else — the listing is the membership test. This deliberately KEEPS both
    Chubb (iso_country='CH') and the China micro-caps AEHL/GDHG (iso_country=
    'CN'): all three are ordinary SHAREs primary-listed in the US. FactSet has
    no field that separates a legitimate foreign large-cap from a foreign
    micro-cap (only size, which this datashare lacks), so we do not try.
  - reg == prim == 'SHARE' is what excludes everything that is NOT single-class
    common stock, with no domicile needed:
      * ADRs split (BABA/JD: regional='ADR', primary='SHARE') -> unequal -> drop
      * ADR-both (SIFY: 'ADR'/'ADR') -> equal but != SHARE -> drop
      * ETFs ('ETF_ETF'/'ETF_ETF'), mutual funds (MF_O/MF_C), preferreds
        (PREF/SHARE), warrants/units/rights/structured -> drop
    Only a security whose regional line and its primary common-share line are
    BOTH plain 'SHARE' survives.

Resolution chain (why we hop THROUGH fsym_primary_equity_id, never filter on it):
  ce_sec_entity links an issuer to EVERY security it issues (Goldman has ~780
  lines: notes, preferreds). An alphabetical pick lands on placeholder tickers
  (AASNXXX-US) or foreign DR lines (AMZN-AR). Hop through
  sym_coverage.fsym_primary_equity_id to the canonical common share, whose
  fsym_regional_id gives the primary ticker and whose fsym_primary_listing_id
  gives THE primary exchange.
"""

# --- universe constants (the ONLY place these values are defined) ------------
US_PRIMARY_EXCHANGES = ("NAS", "NYS", "ASE", "PSE")   # NYSE Arca (PSE) for completeness
EQUITY_SECURITY_TYPE = "SHARE"                          # single-class common shares only

_EXCHANGES_SQL = ", ".join("'%s'" % e for e in US_PRIMARY_EXCHANGES)

# Shared FROM/JOIN chain resolving an ER event's issuer to its PRIMARY common
# share (aliases: e=events, ec=coverage, ent=entity, cse=sec_entity,
# anysec=any issued security, prim=primary common share, reg=regional line,
# lst=primary listing, tr=ticker_region).
_RESOLUTION_JOINS = """
from factset_ce_events.evt_v1.ce_events e
join factset_ce_events.evt_v1.ce_events_coverage ec
    on ec.event_id = e.event_id
join factset_ce_events.sym_v1.sym_entity ent
    on ent.factset_entity_id = ec.factset_entity_id
join factset_ce_events.evt_v1.ce_sec_entity cse
    on cse.factset_entity_id = ec.factset_entity_id
join factset_ce_events.sym_v1.sym_coverage anysec
    on anysec.fsym_id = cse.fsym_id
join factset_ce_events.sym_v1.sym_coverage prim
    on prim.fsym_id = anysec.fsym_primary_equity_id
join factset_ce_events.sym_v1.sym_coverage reg
    on reg.fsym_id = prim.fsym_regional_id
join factset_ce_events.sym_v1.sym_coverage lst
    on lst.fsym_id = reg.fsym_primary_listing_id
join factset_ce_events.sym_v1.sym_ticker_region tr
    on tr.fsym_id = prim.fsym_regional_id
"""

# The universe predicate — ONE rule, applied identically to reporters and peers:
# primary listing on a US exchange AND regional==primary security type=='SHARE'.
# No domicile filter (US-listed foreign issuers are in-universe). The reg==prim
# equality is what drops ADRs (regional 'ADR' != primary 'SHARE'); requiring both
# == 'SHARE' drops ETFs/funds/preferreds/warrants/units.
_UNIVERSE_PREDICATE = """  and lst.fref_listing_exchange in ({exchanges})
  and reg.fref_security_type = '{sectype}'
  and prim.fref_security_type = '{sectype}'""".format(
    exchanges=_EXCHANGES_SQL, sectype=EQUITY_SECURITY_TYPE)


def reporter_events_sql() -> str:
    """SQL (with {start}/{end} placeholders) for fetch-ready ER events.

    Universe predicate + event-only predicates (ER, confirmed, single UTC day).
    projected = false: only CONFIRMED releases — projected events carry
    placeholder datetimes FactSet later revises, corrupting the [-2,+5] window.
    """
    return """
select distinct
    e.event_id,
    e.event_datetime_utc::varchar as event_datetime_utc,
    ent.entity_proper_name,
    tr.ticker_region
{joins}
where e.event_type = 'ER'
  and e.projected = false
{universe}
  and e.event_datetime_utc >= '{{start}}'
  and e.event_datetime_utc <  '{{end}}'
order by event_id
""".format(joins=_RESOLUTION_JOINS.strip(), universe=_UNIVERSE_PREDICATE)


# Peer universe: resolve a set of <SYM>-US tickers directly (no event context)
# and return each one's exchange + BOTH security-type lines so the caller can
# apply the SAME predicate reporters use (US exchange AND reg==prim=='SHARE').
# Returning both lines is what lets us drop ADRs (regional 'ADR' vs primary
# 'SHARE') identically to the reporter query.
_PEER_UNIVERSE_SQL = """
select
    tr.ticker_region,
    lst.fref_listing_exchange  as listing_exchange,
    reg.fref_security_type     as reg_security_type,
    prim.fref_security_type    as prim_security_type
from factset_ce_events.sym_v1.sym_ticker_region tr
join factset_ce_events.sym_v1.sym_coverage reg
    on reg.fsym_id = tr.fsym_id
join factset_ce_events.sym_v1.sym_coverage prim
    on prim.fsym_id = reg.fsym_primary_equity_id
join factset_ce_events.sym_v1.sym_coverage lst
    on lst.fsym_id = prim.fsym_primary_listing_id
where tr.ticker_region in ({tickers})
"""


def peer_universe_sql(bare_symbols) -> str:
    """SQL resolving <SYM>-US for each bare symbol to its universe attributes."""
    tickers = ", ".join("'%s-US'" % s.replace("'", "") for s in bare_symbols)
    return _PEER_UNIVERSE_SQL.format(tickers=tickers)


def in_universe(listing_exchange, reg_security_type, prim_security_type) -> bool:
    """True iff a security is in the US common-equity universe: US-listed and its
    regional line and primary common-share line are BOTH plain 'SHARE'.

    Identical rule to the reporter query. Drops ADRs (reg 'ADR' != prim 'SHARE'),
    ETFs/funds/preferreds/warrants/units (not 'SHARE'). No domicile test —
    US-listed foreign issuers are in-universe. Tolerates NULLs: any None value
    fails the equality and returns False. Unresolved symbols never reach here
    (caller fail-opens on them)."""
    return (listing_exchange in US_PRIMARY_EXCHANGES
            and reg_security_type == EQUITY_SECURITY_TYPE
            and prim_security_type == EQUITY_SECURITY_TYPE)
