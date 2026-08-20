# contoso-data-product-fabric-airflow-builtin

**Reserved.** This repository is a cell in the Contoso family matrix that has
not been built yet: **Fabric**, orchestrated by **the built-in ApacheAirflowJob (Airflow 2)**.

It holds nothing but this file and a LICENSE on purpose. A reserved repository
costs nothing and makes the shape of the family legible; a *witnessed* cell is
the expensive thing, and one is only built when it proves something no existing
cell proves.

## What the product contains

The SQL is not here. It lives in the core so seven leaves cannot drift into
seven versions of it, and that costs you a click, so this list gives it back.
`make show-product` copies the same files into `product/` where you can open
them; the block below is generated from the pinned package and a test fails
when it falls behind.

<!-- BEGIN product inventory: python -m contoso_product.show --markdown -->

The product is [`contoso-data-product`](https://github.com/calvinchengx/contoso-data-product/tree/v0.5.0) at **v0.5.0**, the version this repository pins. It is not vendored here: these files live there and are staged locally by `make show-product`.

**silver**: 8 models, 1 singular test

- [`silver_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_customers.sql)
- [`silver_fx_daily`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_fx_daily.sql)
- [`silver_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_orders.sql)
- [`silver_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_party.sql)
- [`silver_product_hierarchy`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_product_hierarchy.sql)
- [`silver_quarantine_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_quarantine_orders.sql)
- [`silver_web_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_web_customers.sql)
- [`silver_web_order_lines`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_web_order_lines.sql)

Assertions over silver, each failing the build on its own:

- [`silver_orders_never_holds_a_non_positive_quantity`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/tests/silver_orders_never_holds_a_non_positive_quantity.sql)

**gold**: 9 models, 5 singular tests

- [`dim_country`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_country.sql)
- [`dim_customer`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_customer.sql)
- [`dim_date`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_date.sql)
- [`dim_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_party.sql)
- [`dim_product`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_product.sql)
- [`fct_daily_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_daily_revenue.sql)
- [`fct_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_orders.sql)
- [`fct_revenue_summary`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_revenue_summary.sql)
- [`fct_sales`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_sales.sql)

Assertions over gold, each failing the build on its own:

- [`both_selling_systems_reach_the_pack`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/both_selling_systems_reach_the_pack.sql)
- [`every_country_resolves_to_the_dimension`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/every_country_resolves_to_the_dimension.sql)
- [`fiscal_year_is_not_the_calendar_year`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/fiscal_year_is_not_the_calendar_year.sql)
- [`money_is_never_stored_as_float`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/money_is_never_stored_as_float.sql)
- [`revenue_summary_loses_no_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/revenue_summary_loses_no_revenue.sql)

<!-- END product inventory -->

## What will live here

Exactly what a team on this platform would write, and nothing else:

a DAG in the Airflow 2.x idiom, distinct from the external Airflow 3 leaf.

## What will never live here

Transform SQL, an ODCS contract, or an expected number. Those live once, in
[contoso-data-product](https://github.com/calvinchengx/contoso-data-product),
and this leaf will depend on it **by tag**. A second copy of a gold model is
how "one data product, many engines" stops being true.

## Its platform

[fabric-platform-airflow-builtin](https://github.com/calvinchengx/fabric-platform-airflow-builtin)
stands up the infrastructure and runs this leaf. The platform carries no
Contoso name; this leaf carries no infrastructure.

## Where this fits

- [The family](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/00-family.md) — the matrix and the four tiers
- [The plan](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/01-plan.md) — where every cell stands, and what blocks this one
