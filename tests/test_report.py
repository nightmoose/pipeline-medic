"""report.py renders the incident table + KPI cards into the summary.html.j2 template."""

from pipeline_medic.report import Incident, render_summary

INCIDENTS = [
    Incident(
        number=1,
        job="stg_orders",
        failure="Binder error",
        root_cause="customer_id renamed to cust_id",
        status="pipeline-medic/upstream_rename · verified",
        status_kind="verified",
    ),
    Incident(
        number=2,
        job="stg_customers",
        failure="not_null_test:email failed",
        root_cause="upstream feed sends null emails",
        status="suggested · needs owner",
        status_kind="suggested",
    ),
]


def test_render_summary_includes_kpis_and_rows():
    html = render_summary(INCIDENTS, [1.2, 3.4])

    assert "Pipeline Medic" in html
    assert "stg_orders" in html
    assert "stg_customers" in html
    assert '<div class="value">2</div><div class="label">diagnosed</div>' in html
    assert '<div class="value">1</div><div class="label">verified in sandbox</div>' in html
    assert "2.3s" in html  # median of 1.2 and 3.4
    assert "pill verified" in html
    assert "pill suggested" in html


def test_render_summary_handles_no_incidents():
    html = render_summary([], [])

    assert '<div class="value">0</div><div class="label">diagnosed</div>' in html
    assert "n/a" in html
