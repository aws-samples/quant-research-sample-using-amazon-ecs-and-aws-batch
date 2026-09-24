"""Integration test: resolve an ER event to its PRIMARY listing via symbology.

Pins down the correct linkage through the FactSet datashare, discovered
2026-07-29 against event 1203024235 (Amazon Q4 2024 earnings release):

    ce_events -> ce_events_coverage -> ce_sec_entity
      -> sym_coverage (any linked security line)
      -> sym_coverage.fsym_primary_equity_id   (canonical common share)
      -> sym_coverage.fsym_regional_id         (primary regional line -> ticker)
      -> sym_coverage.fsym_primary_listing_id  (THE primary listing -> exchange)
      -> ref_v2.fref_sec_exchange_map          (exchange name / ISO MIC)

Why this matters: ce_sec_entity links an entity to EVERY security line it
issues (DRs, CEDEARs, notes). The manifest SQL's alphabetical row_number()
pick lands on e.g. AMZN-AR (Buenos Aires CEDEAR) for Amazon. The primary-
equity/primary-listing pointers are FactSet's own labels for the real
listing, and must be hopped THROUGH, not filtered on: for some entities
(Exxon) ce_sec_entity contains only derivative lines, so requiring
`fsym_id = fsym_primary_equity_id` on the linked security drops them.

Runs against live Redshift (the datashare workgroup); skips when the datashare
is unreachable. Run explicitly with:

    pytest test_primary_listing_linkage.py -v
"""

import botocore.exceptions
import pytest

from redshift_client import RedshiftClient

import settings

# Amazon.com Q4 2024 earnings release, 2025-02-06 21:01 UTC
AMZN_EVENT_ID = 1203024235

PRIMARY_LISTING_SQL = """
select distinct
    e.event_id,
    e.event_type,
    e.event_datetime_utc::varchar as event_datetime_utc,
    e.url_pr,
    ec.factset_entity_id,
    ent.entity_proper_name,
    ent.iso_country,
    tr.ticker_region as primary_ticker_region,
    lst.fref_listing_exchange as exch_code,
    xm.fref_exchange_desc as exchange_name,
    xm.iso_mic
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
left join factset_ce_events.ref_v2.fref_sec_exchange_map xm
    on xm.fref_exchange_code = lst.fref_listing_exchange
where e.event_id = {event_id}
"""


@pytest.fixture(scope="module")
def client():
    try:
        c = RedshiftClient(workgroup=settings.get("redshift", "workgroup"),
                           database=settings.get("redshift", "database"),
                           secret_arn=settings.get("redshift", "secret_arn"))
        c.execute("select 1", timeout_s=60)
    except (botocore.exceptions.BotoCoreError,
            botocore.exceptions.ClientError,
            RuntimeError, TimeoutError, settings.SettingsError) as e:
        pytest.skip(f"datashare Redshift unreachable: {e}")
    return c


def resolve(client, event_id):
    return list(client.fetch_all(
        PRIMARY_LISTING_SQL.format(event_id=event_id), timeout_s=300))


def test_amzn_event_resolves_to_single_primary_listing(client):
    rows = resolve(client, AMZN_EVENT_ID)

    # Amazon has ~12 security lines (CEDEARs, Thai/Brazilian/Canadian DRs);
    # all must collapse to one primary resolution with no row_number() pick.
    assert len(rows) == 1
    r = rows[0]

    assert r["event_id"] == AMZN_EVENT_ID
    assert r["event_type"] == "ER"
    assert r["event_datetime_utc"] == "2025-02-06 21:01:00"
    assert r["factset_entity_id"] == "001MF1-E"
    assert r["entity_proper_name"] == "Amazon.com, Inc."
    assert r["iso_country"] == "US"

    # the point of the linkage: NASDAQ common share, not the AMZN-AR CEDEAR
    # that the alphabetical pick in the er-full manifest lands on
    assert r["primary_ticker_region"] == "AMZN-US"
    assert r["exch_code"] == "NAS"
    assert r["exchange_name"] == "NASDAQ"
    assert r["iso_mic"] == "XNAS"


def test_dr_only_entity_recovers_primary_listing(client):
    """Exxon: ce_sec_entity holds only DR/preferred lines (no common share),
    so the primary-equity pointer must be hopped through, never filtered on.
    Event 1203013497 is Exxon's Q4 2024 release (2025-01-31)."""
    rows = resolve(client, 1203013497)

    assert len(rows) == 1
    r = rows[0]
    assert r["entity_proper_name"] == "Exxonmobil Corp."
    assert r["primary_ticker_region"] == "XOM-US"
    assert r["exchange_name"] == "New York Stock Exchange"
    assert r["iso_mic"] == "XNYS"


def test_extreme_fanout_and_no_url_vintage(client):
    """Goldman Sachs: the pathological fan-out — the entity links to ~780
    security lines in ce_sec_entity (structured notes, preferreds, DRs),
    all of which must collapse to the single GS-US / NYSE resolution.
    Event 122343 is GS Q3 2005 (fiscal year ended November then). As of
    2026-07 its url_pr is null — typical pre-~2010 vintage, which is why
    such events are absent from the er-full S3 manifest — but that's
    FactSet coverage, not linkage, so it is deliberately NOT asserted."""
    rows = resolve(client, 122343)

    assert len(rows) == 1
    r = rows[0]
    assert r["event_type"] == "ER"
    assert r["event_datetime_utc"] == "2005-09-20 00:00:00"
    assert r["factset_entity_id"] == "002615-E"
    assert r["entity_proper_name"] == "The Goldman Sachs Group, Inc."
    assert r["iso_country"] == "US"
    assert r["primary_ticker_region"] == "GS-US"
    assert r["exch_code"] == "NYS"
    assert r["exchange_name"] == "New York Stock Exchange"
    assert r["iso_mic"] == "XNYS"

    fanout = list(client.fetch_all(
        "select count(*) as n from factset_ce_events.evt_v1.ce_sec_entity"
        " where factset_entity_id = '002615-E'", timeout_s=120))
    assert fanout[0]["n"] > 500