import datetime
import tempfile
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
import sqlglot
import streamlit as st

import ibis
from ibis import _

ONE_HOUR_IN_SECONDS = datetime.timedelta(hours=1).total_seconds()

st.set_page_config(layout='wide')


def support_matrix_df():
    resp = requests.get("https://raw.githubusercontent.com/richtia/substrait/streamlit_test_report/site/docs/producer_function_compatibility/producer_results.csv")
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile() as f:
        f.write(resp.content)
        return (
            ibis.read_csv(f.name)
            .relabel({'FullFunction': 'full_function'})
            .mutate(
                function_category=_.full_function.split(".")[-2],
            )
            .execute()
        )


def backends_info_df():
    return pd.DataFrame(
        {
            "DuckDBProducer": ["string", "sql"],
            "IbisProducer": ["string", "sql"],
            'IsthmusProducer': ["dataframe"],
        }.items(),
        columns=['backend_name', 'categories'],
    )


backend_info_table = ibis.memtable(backends_info_df())
support_matrix_table = ibis.memtable(support_matrix_df())


def get_all_function_categories():
    return (
        support_matrix_table.select(_.function_category)
        .distinct()['function_category']
        .execute()
        .tolist()
    )


def get_backend_names(categories: Optional[List[str]] = None):
    backend_expr = backend_info_table.mutate(category=_.categories.unnest())
    if categories:
        backend_expr = backend_expr.filter(_.category.isin(categories))
    return (
        backend_expr.select(_.backend_name).distinct().backend_name.execute().tolist()
    )


def get_selected_backend_name():
    # backend_categories = get_all_backend_categories()
    # selected_categories_names = st.sidebar.multiselect(
    #     'Backend category',
    #     options=backend_categories,
    #     default=None,
    # )
    # if not selected_categories_names:
    return get_backend_names()
    # return get_backend_names()


def get_selected_function_categories():
    all_ops_categories = get_all_function_categories()

    selected_ops_categories = st.sidebar.multiselect(
        'Function category',
        options=sorted(all_ops_categories),
        default=None,
    )
    if not selected_ops_categories:
        selected_ops_categories = all_ops_categories
    return selected_ops_categories


current_backend_names = get_selected_backend_name()
# sort_by_coverage = st.sidebar.checkbox('Sort by API Coverage', value=False)
current_ops_categories = get_selected_function_categories()

# Start ibis expression
table_expr = support_matrix_table

# Add index to result
table_expr = table_expr.mutate(index=_.full_operation)
table_expr = table_expr.order_by(_.index)

# Filter functions by selected categories
table_expr = table_expr.filter(_.function_category.isin(current_ops_categories))

# Filter operation by compatibility
supported_backend_count = sum(
    getattr(table_expr, backend_name).ifelse(1, 0)
    for backend_name in current_backend_names
)

# Show only selected backend
table_expr = table_expr[current_backend_names + ["index"]]

# Execute query
df = table_expr.execute()
df = df.set_index('index')

# Display result
all_visible_ops_count = len(df.index)
if all_visible_ops_count:
    # Compute coverage
    coverage = (
        df.sum()
        .sort_values(ascending=False)
        .map(lambda n: f"{n} ({round(100 * n / all_visible_ops_count)}%)")
        .to_frame(name="API Coverage")
        .T
    )

    table = pd.concat([coverage, df.replace({True: "✔", False: "🚫"})]).loc[
        :, sorted(df.columns)
    ]
    st.dataframe(table)
else:
    st.write("No data")

