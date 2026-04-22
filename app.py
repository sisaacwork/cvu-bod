"""
CVU Buildings of Distinction — Title Finder
============================================
A Streamlit app to explore and filter potential BoD titles
by building, function, material, geography, and company.

Run locally with:
    streamlit run app.py

Credentials go in .streamlit/secrets.toml (see secrets.toml.example).
"""

import streamlit as st
import pandas as pd
import mysql.connector

# ─────────────────────────────────────────────────────────────────────────────
# PAGE SETUP
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CVU BoD Title Finder",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _connect():
    """Open a fresh MySQL connection using credentials from Streamlit secrets."""
    cfg = st.secrets["mysql"]
    return mysql.connector.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        connection_timeout=30,
    )


def run_query(sql: str, params: list | tuple | None = None) -> pd.DataFrame:
    """
    Run a SQL query and return a pandas DataFrame.
    Opens and closes a connection per call — safe for cached functions.
    """
    conn = _connect()
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df





# ─────────────────────────────────────────────────────────────────────────────
# MAIN TITLES QUERY  (global — no region/city/height restrictions on candidates)
# ─────────────────────────────────────────────────────────────────────────────
#
# How it works (plain English):
#   1. Pull every completed building in the world as a "candidate".
#   2. Expand each candidate's functions (up to 5 uses).  If a building has
#      more than one distinct use, it's labelled "mixed-use"; otherwise it gets
#      its single function label.
#   3. Do the same expansion for structural material.
#   4. For RANKING purposes, do the same expansion across ALL COM buildings
#      globally (all_func / all_mat).  This ensures ranks are accurate —
#      e.g. "Tallest Office Building in Seoul" is ranked against every office
#      building in Seoul, not just the filtered candidates.
#   5. In the final SELECT, combine eight title buckets:
#        Function × {City, Country, Region, World}
#        Material × {City, Country, Region, World}
#      Each bucket uses a window function (ROW_NUMBER) to assign ranks,
#      then keeps ranks 1–5.
#
TITLES_SQL = """
WITH
-- ── 1. Every completed building globally ─────────────────────────────────────
candidates AS (
    SELECT
        b.id, b.name_intl, b.height_architecture, b.completed,
        b.structural_material,
        b.main_use_01, b.main_use_02, b.main_use_03, b.main_use_04, b.main_use_05,
        b.city_id, b.country_id, b.region_id,
        ci.name AS city_name,
        co.name AS country_name,
        r.name  AS region_name
    FROM ctbuh_building b
    LEFT JOIN v2_cities    ci ON b.city_id    = ci.id
    LEFT JOIN v2_countries co ON b.country_id = co.id
    LEFT JOIN v2_regions   r  ON b.region_id  = r.id
    WHERE b.status          = 'COM'
      AND b.structure_type  = 'building'
      AND b.deleted_at      IS NULL
      AND b.height_architecture > 0
),

-- ── 2. Candidate functions (mixed-use logic) ──────────────────────────────────
cand_func AS (
    SELECT DISTINCT
        r.id, r.name_intl, r.height_architecture, r.completed,
        r.city_id, r.city_name, r.country_id, r.country_name, r.region_id, r.region_name,
        CASE WHEN fc.num_funcs > 1 THEN 'mixed-use' ELSE r.func END AS func
    FROM (
        SELECT id, name_intl, height_architecture, completed, city_id, city_name, country_id, country_name, region_id, region_name, main_use_01 AS func FROM candidates WHERE main_use_01 <> ''
        UNION ALL
        SELECT id, name_intl, height_architecture, completed, city_id, city_name, country_id, country_name, region_id, region_name, main_use_02 FROM candidates WHERE main_use_02 <> ''
        UNION ALL
        SELECT id, name_intl, height_architecture, completed, city_id, city_name, country_id, country_name, region_id, region_name, main_use_03 FROM candidates WHERE main_use_03 <> ''
        UNION ALL
        SELECT id, name_intl, height_architecture, completed, city_id, city_name, country_id, country_name, region_id, region_name, main_use_04 FROM candidates WHERE main_use_04 <> ''
        UNION ALL
        SELECT id, name_intl, height_architecture, completed, city_id, city_name, country_id, country_name, region_id, region_name, main_use_05 FROM candidates WHERE main_use_05 <> ''
    ) r
    JOIN (
        SELECT id, COUNT(DISTINCT func) AS num_funcs
        FROM (
            SELECT id, main_use_01 AS func FROM candidates WHERE main_use_01 <> ''
            UNION ALL SELECT id, main_use_02 FROM candidates WHERE main_use_02 <> ''
            UNION ALL SELECT id, main_use_03 FROM candidates WHERE main_use_03 <> ''
            UNION ALL SELECT id, main_use_04 FROM candidates WHERE main_use_04 <> ''
            UNION ALL SELECT id, main_use_05 FROM candidates WHERE main_use_05 <> ''
        ) uses
        GROUP BY id
    ) fc ON r.id = fc.id
),

-- ── 3. Candidate materials with display labels ────────────────────────────────
cand_mat AS (
    SELECT
        id, name_intl, height_architecture, completed,
        city_id, city_name, country_id, country_name, region_id, region_name,
        structural_material AS mat,
        CASE structural_material
            WHEN 'concrete'                  THEN 'Concrete'
            WHEN 'steel'                     THEN 'Steel'
            WHEN 'composite'                 THEN 'Composite'
            WHEN 'masonry'                   THEN 'Masonry'
            WHEN 'precast'                   THEN 'Precast Concrete'
            WHEN 'timber'                    THEN 'Timber'
            WHEN 'timber/concrete'           THEN 'Timber/Concrete'
            WHEN 'timber composite/concrete' THEN 'Timber-Composite/Concrete'
            WHEN 'timber/composite'          THEN 'Timber/Composite'
            WHEN 'steel/concrete'            THEN 'Steel/Concrete'
            WHEN 'concrete/steel'            THEN 'Concrete/Steel'
            ELSE structural_material
        END AS mat_label
    FROM candidates
    WHERE structural_material <> ''
),

-- ── 4. ALL global COM buildings by function (for accurate ranking) ────────────
all_func AS (
    SELECT DISTINCT
        r.id, b2.city_id, b2.country_id, b2.region_id, b2.height_architecture,
        CASE WHEN fc.num_funcs > 1 THEN 'mixed-use' ELSE r.func END AS func
    FROM (
        SELECT b.id, b.main_use_01 AS func FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_01<>''
        UNION ALL SELECT b.id, b.main_use_02 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_02<>''
        UNION ALL SELECT b.id, b.main_use_03 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_03<>''
        UNION ALL SELECT b.id, b.main_use_04 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_04<>''
        UNION ALL SELECT b.id, b.main_use_05 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_05<>''
    ) r
    JOIN ctbuh_building b2 ON r.id = b2.id
    JOIN (
        SELECT id, COUNT(DISTINCT func) AS num_funcs
        FROM (
            SELECT b.id, b.main_use_01 AS func FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_01<>''
            UNION ALL SELECT b.id, b.main_use_02 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_02<>''
            UNION ALL SELECT b.id, b.main_use_03 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_03<>''
            UNION ALL SELECT b.id, b.main_use_04 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_04<>''
            UNION ALL SELECT b.id, b.main_use_05 FROM ctbuh_building b WHERE b.status='COM' AND b.structure_type='building' AND b.deleted_at IS NULL AND b.height_architecture>0 AND b.main_use_05<>''
        ) uses
        GROUP BY id
    ) fc ON r.id = fc.id
),

-- ── 5. ALL global COM buildings by material (for accurate ranking) ────────────
all_mat AS (
    SELECT b.id, b.city_id, b.country_id, b.region_id, b.height_architecture,
           b.structural_material AS mat
    FROM ctbuh_building b
    WHERE b.status = 'COM' AND b.structure_type = 'building' AND b.deleted_at IS NULL
      AND b.height_architecture > 0 AND b.structural_material <> ''
),

-- ── 6. ALL global COM buildings for overall height ranking ────────────────────
-- No function or material filter — every completed building qualifies.
all_overall AS (
    SELECT b.id, b.city_id, b.country_id, b.region_id, b.height_architecture
    FROM ctbuh_building b
    WHERE b.status = 'COM' AND b.structure_type = 'building' AND b.deleted_at IS NULL
      AND b.height_architecture > 0
)

-- ── FINAL: combine all twelve title buckets ───────────────────────────────────
SELECT
    results.title,
    results.title_type,
    results.category,
    results.geography_level,
    results.geography,
    results.city_name,
    results.country_name,
    results.building_id,
    results.building_name,
    results.height_architecture,
    results.completed,
    results.rank_in_category,
    -- Developer / Owner column (category_id 1 = Owner, 2 = Developer)
    (SELECT GROUP_CONCAT(
         CONCAT(co.name, ' (',
             CASE bc.category_id
                 WHEN 1 THEN CONCAT('Owner', CASE WHEN sc.name IS NOT NULL THEN CONCAT(' - ', sc.name) ELSE '' END)
                 WHEN 2 THEN 'Developer'
             END,
         ')')
         ORDER BY bc.category_id, bc.display_order SEPARATOR '; ')
     FROM ctbuh_building_company_new bc
     JOIN ctbuh_company co ON bc.company_id = co.id
     LEFT JOIN ctbuh_company_subcategories sc ON bc.subcategory_id = sc.id
     WHERE bc.building_id = results.building_id AND bc.category_id IN (1,2) AND bc.retrofit = 0
    ) AS developer_owner,
    -- Architect column (category_id 3)
    (SELECT GROUP_CONCAT(
         CONCAT(co.name, ' (', COALESCE(sc.name, 'Architect'), ')')
         ORDER BY bc.display_order SEPARATOR '; ')
     FROM ctbuh_building_company_new bc
     JOIN ctbuh_company co ON bc.company_id = co.id
     LEFT JOIN ctbuh_company_subcategories sc ON bc.subcategory_id = sc.id
     WHERE bc.building_id = results.building_id AND bc.category_id = 3 AND bc.retrofit = 0
    ) AS architect
FROM (
    -- ── Function × City ───────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END, ' ',
            CASE cf.func WHEN 'mixed-use' THEN 'Mixed-Use'
                         ELSE CONCAT(UPPER(LEFT(cf.func,1)), SUBSTRING(cf.func,2)) END,
            ' Building in ', cf.city_name
        ) AS title,
        'Function' AS title_type, cf.func AS category,
        'City' AS geography_level, cf.city_name AS geography,
        cf.id AS building_id, cf.name_intl AS building_name,
        cf.city_name, cf.country_name,
        cf.height_architecture, cf.completed, cr.rnk AS rank_in_category
    FROM cand_func cf
    JOIN (
        SELECT id, func, city_id,
               ROW_NUMBER() OVER (PARTITION BY func, city_id ORDER BY height_architecture DESC) AS rnk
        FROM all_func
    ) cr ON cf.id = cr.id AND cf.func = cr.func AND cf.city_id = cr.city_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Function × Country ────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END, ' ',
            CASE cf.func WHEN 'mixed-use' THEN 'Mixed-Use'
                         ELSE CONCAT(UPPER(LEFT(cf.func,1)), SUBSTRING(cf.func,2)) END,
            ' Building in ', cf.country_name
        ),
        'Function', cf.func, 'Country', cf.country_name,
        cf.id, cf.name_intl, cf.city_name, cf.country_name, cf.height_architecture, cf.completed, cr.rnk
    FROM cand_func cf
    JOIN (
        SELECT id, func, country_id,
               ROW_NUMBER() OVER (PARTITION BY func, country_id ORDER BY height_architecture DESC) AS rnk
        FROM all_func
    ) cr ON cf.id = cr.id AND cf.func = cr.func AND cf.country_id = cr.country_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Function × Region ─────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END, ' ',
            CASE cf.func WHEN 'mixed-use' THEN 'Mixed-Use'
                         ELSE CONCAT(UPPER(LEFT(cf.func,1)), SUBSTRING(cf.func,2)) END,
            ' Building in ', cf.region_name
        ),
        'Function', cf.func, 'Region', cf.region_name,
        cf.id, cf.name_intl, cf.city_name, cf.country_name, cf.height_architecture, cf.completed, cr.rnk
    FROM cand_func cf
    JOIN (
        SELECT id, func, region_id,
               ROW_NUMBER() OVER (PARTITION BY func, region_id ORDER BY height_architecture DESC) AS rnk
        FROM all_func
    ) cr ON cf.id = cr.id AND cf.func = cr.func AND cf.region_id = cr.region_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Function × World ──────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END, ' ',
            CASE cf.func WHEN 'mixed-use' THEN 'Mixed-Use'
                         ELSE CONCAT(UPPER(LEFT(cf.func,1)), SUBSTRING(cf.func,2)) END,
            ' Building in the World'
        ),
        'Function', cf.func, 'World', 'World',
        cf.id, cf.name_intl, cf.city_name, cf.country_name, cf.height_architecture, cf.completed, cr.rnk
    FROM cand_func cf
    JOIN (
        SELECT id, func,
               ROW_NUMBER() OVER (PARTITION BY func ORDER BY height_architecture DESC) AS rnk
        FROM all_func
    ) cr ON cf.id = cr.id AND cf.func = cr.func
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Material × City ───────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' ', cm.mat_label, ' Building in ', cm.city_name
        ),
        'Material', cm.mat, 'City', cm.city_name,
        cm.id, cm.name_intl, cm.city_name, cm.country_name, cm.height_architecture, cm.completed, cr.rnk
    FROM cand_mat cm
    JOIN (
        SELECT id, mat, city_id,
               ROW_NUMBER() OVER (PARTITION BY mat, city_id ORDER BY height_architecture DESC) AS rnk
        FROM all_mat
    ) cr ON cm.id = cr.id AND cm.mat = cr.mat AND cm.city_id = cr.city_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Material × Country ────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' ', cm.mat_label, ' Building in ', cm.country_name
        ),
        'Material', cm.mat, 'Country', cm.country_name,
        cm.id, cm.name_intl, cm.city_name, cm.country_name, cm.height_architecture, cm.completed, cr.rnk
    FROM cand_mat cm
    JOIN (
        SELECT id, mat, country_id,
               ROW_NUMBER() OVER (PARTITION BY mat, country_id ORDER BY height_architecture DESC) AS rnk
        FROM all_mat
    ) cr ON cm.id = cr.id AND cm.mat = cr.mat AND cm.country_id = cr.country_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Material × Region ─────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' ', cm.mat_label, ' Building in ', cm.region_name
        ),
        'Material', cm.mat, 'Region', cm.region_name,
        cm.id, cm.name_intl, cm.city_name, cm.country_name, cm.height_architecture, cm.completed, cr.rnk
    FROM cand_mat cm
    JOIN (
        SELECT id, mat, region_id,
               ROW_NUMBER() OVER (PARTITION BY mat, region_id ORDER BY height_architecture DESC) AS rnk
        FROM all_mat
    ) cr ON cm.id = cr.id AND cm.mat = cr.mat AND cm.region_id = cr.region_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Material × World ──────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' ', cm.mat_label, ' Building in the World'
        ),
        'Material', cm.mat, 'World', 'World',
        cm.id, cm.name_intl, cm.city_name, cm.country_name, cm.height_architecture, cm.completed, cr.rnk
    FROM cand_mat cm
    JOIN (
        SELECT id, mat,
               ROW_NUMBER() OVER (PARTITION BY mat ORDER BY height_architecture DESC) AS rnk
        FROM all_mat
    ) cr ON cm.id = cr.id AND cm.mat = cr.mat
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Overall × City ────────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' Building in ', c.city_name
        ) AS title,
        'Overall' AS title_type, 'overall' AS category,
        'City' AS geography_level, c.city_name AS geography,
        c.id AS building_id, c.name_intl AS building_name,
        c.city_name, c.country_name,
        c.height_architecture, c.completed, cr.rnk AS rank_in_category
    FROM candidates c
    JOIN (
        SELECT id, city_id,
               ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY height_architecture DESC) AS rnk
        FROM all_overall
    ) cr ON c.id = cr.id AND c.city_id = cr.city_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Overall × Country ─────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' Building in ', c.country_name
        ),
        'Overall', 'overall', 'Country', c.country_name,
        c.id, c.name_intl, c.city_name, c.country_name,
        c.height_architecture, c.completed, cr.rnk
    FROM candidates c
    JOIN (
        SELECT id, country_id,
               ROW_NUMBER() OVER (PARTITION BY country_id ORDER BY height_architecture DESC) AS rnk
        FROM all_overall
    ) cr ON c.id = cr.id AND c.country_id = cr.country_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Overall × Region ──────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' Building in ', c.region_name
        ),
        'Overall', 'overall', 'Region', c.region_name,
        c.id, c.name_intl, c.city_name, c.country_name,
        c.height_architecture, c.completed, cr.rnk
    FROM candidates c
    JOIN (
        SELECT id, region_id,
               ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY height_architecture DESC) AS rnk
        FROM all_overall
    ) cr ON c.id = cr.id AND c.region_id = cr.region_id
    WHERE cr.rnk <= 5

    UNION ALL
    -- ── Overall × World ───────────────────────────────────────────────────────
    SELECT
        CONCAT(
            CASE cr.rnk WHEN 1 THEN 'Tallest' WHEN 2 THEN 'Second-Tallest'
                        WHEN 3 THEN 'Third-Tallest' WHEN 4 THEN 'Fourth-Tallest'
                        WHEN 5 THEN 'Fifth-Tallest' END,
            ' Building in the World'
        ),
        'Overall', 'overall', 'World', 'World',
        c.id, c.name_intl, c.city_name, c.country_name,
        c.height_architecture, c.completed, cr.rnk
    FROM candidates c
    JOIN (
        SELECT id,
               ROW_NUMBER() OVER (ORDER BY height_architecture DESC) AS rnk
        FROM all_overall
    ) cr ON c.id = cr.id
    WHERE cr.rnk <= 5
) results
ORDER BY results.building_name, results.title_type, results.geography_level, results.rank_in_category
"""


# Bump this version string any time the SQL schema changes so Streamlit
# immediately invalidates the old cached result on the next page load.
_TITLES_VERSION = "v3-overall-titles"


@st.cache_data(ttl=3600, show_spinner="Loading titles from database — this takes a moment…")
def load_titles(_version: str = _TITLES_VERSION) -> pd.DataFrame:
    """
    Pull all global BoD title candidates (ranks 1–5) from MySQL.
    Cached for 1 hour so filtering is instant after the first load.
    The _version parameter is a cache-buster: change _TITLES_VERSION above
    whenever the SQL columns change, so stale cached data is never used.
    """
    return run_query(TITLES_SQL)


# ─────────────────────────────────────────────────────────────────────────────
# COMPANY DETAIL QUERY  (runs on a single building when a row is selected)
# ─────────────────────────────────────────────────────────────────────────────

COMPANY_SQL = """
SELECT
    bc.category_id,
    CASE bc.category_id
        WHEN 1 THEN 'Owner'
        WHEN 2 THEN 'Developer'
        WHEN 3 THEN 'Architect'
    END AS role,
    COALESCE(sc.name, '') AS subcategory,
    co.id   AS company_id,
    co.name AS company_name,
    CASE
        WHEN m.level IS NULL OR m.level = 0 THEN 'Non-Member'
        ELSE COALESCE(ml.name, 'Member')
    END AS membership_type
FROM ctbuh_building_company_new bc
JOIN  ctbuh_company co             ON bc.company_id    = co.id
LEFT JOIN ctbuh_company_subcategories sc ON bc.subcategory_id = sc.id
LEFT JOIN v2_members m             ON co.id = m.company_id
LEFT JOIN v2_membership_levels ml  ON m.level = ml.id
WHERE bc.building_id = %s
  AND bc.category_id IN (1, 2, 3)
  AND bc.retrofit    = 0
ORDER BY bc.category_id, bc.display_order
"""


@st.cache_data(ttl=3600, show_spinner=False)
def load_companies(building_id: int) -> pd.DataFrame:
    return run_query(COMPANY_SQL, params=(building_id,))


# ─────────────────────────────────────────────────────────────────────────────
# COMPETITOR QUERY  (buildings that could displace the selected title)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def get_competitors(
    title_type: str,
    category: str,
    geography_level: str,
    geography: str,
    holder_height: float,
) -> pd.DataFrame:
    """
    Find UCT / STO / UC / PRO buildings taller than holder_height in the same
    geography + function-or-material bucket.  These are buildings that would
    knock the current holder out of its title once they complete.
    """
    params: list = [holder_height]

    # Geography join + filter
    if geography_level == "City":
        geo_join   = "LEFT JOIN v2_cities    geo_t ON b.city_id    = geo_t.id"
        geo_filter = "AND geo_t.name = %s"
        params.append(geography)
    elif geography_level == "Country":
        geo_join   = "LEFT JOIN v2_countries geo_t ON b.country_id = geo_t.id"
        geo_filter = "AND geo_t.name = %s"
        params.append(geography)
    elif geography_level == "Region":
        geo_join   = "LEFT JOIN v2_regions   geo_t ON b.region_id  = geo_t.id"
        geo_filter = "AND geo_t.name = %s"
        params.append(geography)
    else:  # World — no geographic restriction
        geo_join   = ""
        geo_filter = ""

    # Function or material filter
    if title_type == "Function":
        if category == "mixed-use":
            # Mixed-use = more than one distinct non-empty use field
            func_filter = """
            AND (
                (CASE WHEN b.main_use_01 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_02 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_03 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_04 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_05 <> '' THEN 1 ELSE 0 END) > 1
            )"""
        else:
            # Single-function building containing this specific use
            func_filter = """
            AND (
                b.main_use_01 = %s OR b.main_use_02 = %s
             OR b.main_use_03 = %s OR b.main_use_04 = %s OR b.main_use_05 = %s
            )
            AND (
                (CASE WHEN b.main_use_01 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_02 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_03 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_04 <> '' THEN 1 ELSE 0 END
               + CASE WHEN b.main_use_05 <> '' THEN 1 ELSE 0 END) = 1
            )"""
            params.extend([category] * 5)
    else:  # Material
        func_filter = "AND b.structural_material = %s"
        params.append(category)

    sql = f"""
    SELECT
        b.id,
        b.name_intl AS building_name,
        b.height_architecture,
        CASE WHEN b.completed = 0 THEN NULL ELSE b.completed END AS est_completion,
        CASE b.status
            WHEN 'UCT' THEN 'Topped Out'
            WHEN 'STO' THEN 'Structural Topped Out'
            WHEN 'UC'  THEN 'Under Construction'
            WHEN 'PRO' THEN 'Proposed'
            ELSE b.status
        END AS status_label
    FROM ctbuh_building b
    {geo_join}
    WHERE b.status IN ('UCT', 'STO', 'UC', 'PRO')
      AND b.structure_type  = 'building'
      AND b.deleted_at      IS NULL
      AND b.height_architecture > %s
      {geo_filter}
      {func_filter}
    ORDER BY b.height_architecture DESC
    LIMIT 10
    """
    return run_query(sql, params=params)


# ─────────────────────────────────────────────────────────────────────────────
# FILTER HELPER
# ─────────────────────────────────────────────────────────────────────────────

def apply_filters(
    df: pd.DataFrame,
    building_search: str,
    company_search: str,
    title_types: list,
    categories: list,
    geo_levels: list,
    geo_search: str,
    geographies: list,
    min_height: int,
    max_rank: int,
) -> pd.DataFrame:
    """Apply all sidebar filters to the full titles DataFrame."""
    out = df.copy()

    if building_search.strip():
        out = out[out["building_name"].str.contains(building_search.strip(), case=False, na=False)]

    if company_search.strip():
        s = company_search.strip()
        dev  = out["developer_owner"].fillna("").str.contains(s, case=False, na=False)
        arch = out["architect"].fillna("").str.contains(s, case=False, na=False)
        out  = out[dev | arch]

    if title_types:
        out = out[out["title_type"].isin(title_types)]

    if categories:
        out = out[out["category"].isin(categories)]

    out = out[out["height_architecture"] >= min_height]
    out = out[out["rank_in_category"] <= max_rank]

    # Geography filter — works at the BUILDING level, not the title level.
    # We first find building IDs whose city/country/region matches the search,
    # then return ALL titles for those buildings (so a Guangzhou search also
    # returns that building's China, Asia, and World titles).
    if geo_search.strip() or geographies:
        geo_mask = pd.Series(True, index=out.index)
        if geo_search.strip():
            # Search the city_name and country_name columns (the building's actual
            # location) as well as the geography column, so typing "Guangzhou"
            # catches it whether the title is City, Country, Region, or World level.
            loc_hit = (
                out["city_name"].fillna("").str.contains(geo_search.strip(), case=False)
                | out["country_name"].fillna("").str.contains(geo_search.strip(), case=False)
                | out["geography"].fillna("").str.contains(geo_search.strip(), case=False)
            )
            geo_mask &= loc_hit
        if geographies:
            # Multi-select: match against city_name or country_name so choosing
            # "Guangzhou" from the list finds buildings located there, not just
            # titles whose geography label is "Guangzhou".
            geo_mask &= (
                out["city_name"].isin(geographies)
                | out["country_name"].isin(geographies)
                | out["geography"].isin(geographies)
            )
        matching_ids = out.loc[geo_mask, "building_id"].unique()
        out = out[out["building_id"].isin(matching_ids)]

    # Geography level filter — applied after building identification so that
    # e.g. a Guangzhou search with geo_levels=["World","Country"] returns
    # the China + World titles for Guangzhou buildings, not just City titles.
    if geo_levels:
        out = out[out["geography_level"].isin(geo_levels)]

    # Deduplication: for each (building, title type, category, rank), keep only
    # the highest geography level.  This prevents showing "Tallest Steel Building
    # in the World" AND "Tallest Steel Building in Asia" for the same building —
    # the World title supersedes all narrower geographies at the same rank.
    # Priority: World (0) > Region (1) > Country (2) > City (3).
    _GEO_PRIORITY = {"World": 0, "Region": 1, "Country": 2, "City": 3}
    out["_geo_p"] = out["geography_level"].map(_GEO_PRIORITY)
    out = (
        out.sort_values("_geo_p")
           .drop_duplicates(
               subset=["building_id", "title_type", "category", "rank_in_category"],
               keep="first",
           )
           .drop(columns=["_geo_p"])
    )

    return out.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# DETAIL PANEL  (shown below the table when a row is clicked)
# ─────────────────────────────────────────────────────────────────────────────

def show_detail(row: pd.Series, df_all: pd.DataFrame, show_competitors: bool):
    """Render the detail card for a selected title / building."""
    st.divider()

    # ── Header ───────────────────────────────────────────────────────────────
    col_name, col_stats = st.columns([3, 1])
    with col_name:
        st.subheader(f"🏢 {row['building_name']}")
        year = int(row["completed"]) if pd.notna(row["completed"]) and row["completed"] != 0 else "—"
        st.write(f"**Height:** {row['height_architecture']:.0f} m  ·  **Completed:** {year}")

    with col_stats:
        rank_label = {1: "Tallest", 2: "2nd-Tallest", 3: "3rd-Tallest",
                      4: "4th-Tallest", 5: "5th-Tallest"}.get(int(row["rank_in_category"]), f"#{int(row['rank_in_category'])}")
        st.metric("Rank (this title)", rank_label)
        st.caption(f"**{row['geography_level']}** — {row['geography']}")

    # ── All titles for this building ─────────────────────────────────────────
    st.markdown("**All BoD Titles for this Building**")
    building_titles = (
        df_all[df_all["building_id"] == row["building_id"]][["title", "rank_in_category"]]
        .drop_duplicates()
        .sort_values("rank_in_category")
    )
    for _, t in building_titles.iterrows():
        st.write(f"• {t['title']}")

    # ── Companies & Contacts ─────────────────────────────────────────────────
    st.markdown("### Companies")
    companies = load_companies(int(row["building_id"]))

    if companies.empty:
        st.info("No company data found for this building.")
    else:
        for _, comp in companies.iterrows():
            role_label = comp["role"]
            if comp["subcategory"]:
                role_label += f" — {comp['subcategory']}"

            if comp["membership_type"] == "Non-Member":
                mem_badge = "⚪ Non-Member"
            else:
                mem_badge = f"🟢 {comp['membership_type']}"

            with st.expander(f"**{comp['company_name']}** · {role_label} · {mem_badge}"):
                st.caption(f"Company ID: {comp['company_id']}")

    # ── Competitors ──────────────────────────────────────────────────────────
    if show_competitors:
        st.markdown("### ⚠️ Competitors")
        st.caption(
            "Buildings not yet complete (Topped Out / Under Construction / Proposed) "
            "that are taller and would displace this title on completion."
        )
        comps = get_competitors(
            title_type=row["title_type"],
            category=row["category"],
            geography_level=row["geography_level"],
            geography=row["geography"],
            holder_height=float(row["height_architecture"]),
        )
        if comps.empty:
            st.success("No known competitors for this title.")
        else:
            st.dataframe(
                comps[["building_name", "height_architecture", "status_label", "est_completion"]].rename(columns={
                    "building_name":      "Building",
                    "height_architecture":"Height (m)",
                    "status_label":       "Status",
                    "est_completion":     "Est. Completion",
                }),
                hide_index=True,
                use_container_width=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    st.title("CVU Buildings of Distinction — Title Finder")
    st.caption(
        "Browse, filter, and explore potential BoD titles globally. "
        "Click any row in the table to see companies and competitors."
    )

    # ── Load data ─────────────────────────────────────────────────────────────
    df_all = load_titles(_version=_TITLES_VERSION)

    if df_all.empty:
        st.error("No data returned from the database. Check your .streamlit/secrets.toml credentials.")
        st.stop()

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.header("🔍 Filters")

        building_search = st.text_input("Search building name", placeholder="e.g. Burj Khalifa")
        company_search  = st.text_input("Search by company",    placeholder="e.g. SOM, Emaar")

        st.divider()

        all_types = sorted(df_all["title_type"].dropna().unique().tolist())
        title_types = st.multiselect(
            "Title Type (Function / Material / Overall)",
            options=all_types,
            default=all_types,
            help="Filter by how the title is categorised — by building function or structural material.",
        )

        all_cats = sorted(df_all["category"].dropna().unique().tolist())
        categories = st.multiselect(
            "Function / Material Category",
            options=all_cats,
            default=[],
            placeholder="All categories",
            help="e.g. 'office', 'mixed-use', 'concrete'. Leave blank for all.",
        )

        geo_level_options = ["World", "Region", "Country", "City"]
        geo_levels = st.multiselect(
            "Geography Level",
            options=geo_level_options,
            default=geo_level_options,
            help="Which geographic scope of title to show.",
        )

        # Geography search — text box for quick free-text lookup
        geo_search = st.text_input(
            "Geography search",
            placeholder="e.g. Seoul, United States, Asia",
            help="Partial match against region, country, or city name.",
        )

        # Geography multi-select — populated from actual building locations
        # (city_name + country_name), not from title geography labels.
        # This means "World" and region names never pollute the list, and
        # selecting "Guangzhou" always means buildings physically in Guangzhou.
        city_names    = df_all["city_name"].dropna().unique().tolist()
        country_names = df_all["country_name"].dropna().unique().tolist()
        geo_pool = sorted(set(city_names + country_names) - {""})
        # Narrow pool by whatever is typed in the search box
        if geo_search.strip():
            q = geo_search.strip().lower()
            geo_pool = [g for g in geo_pool if q in g.lower()]

        geographies = st.multiselect(
            "Geographies multi-select",
            options=geo_pool,
            default=[],
            placeholder="All — or pick one / several",
            help="Select one or more specific regions, countries, or cities.",
        )

        st.divider()

        min_height = st.slider(
            "Min. height (m)",
            min_value=0, max_value=800, value=200, step=25,
            help="Only show buildings at or above this height.",
        )

        max_rank = st.slider(
            "Show titles up to rank…",
            min_value=1, max_value=5, value=1, step=1,
            help="1 = Tallest only.  5 = show up to Fifth-Tallest.",
            format="Top %d",
        )

        st.divider()

        show_competitors = st.toggle(
            "Show competitors",
            value=True,
            help="When a building is selected, show non-complete buildings that could displace its title.",
        )

        st.divider()

        if st.button("🔄 Refresh data from database", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # ── Apply filters ─────────────────────────────────────────────────────────
    df = apply_filters(
        df_all,
        building_search=building_search,
        company_search=company_search,
        title_types=title_types,
        categories=categories,
        geo_levels=geo_levels,
        geo_search=geo_search,
        geographies=geographies,
        min_height=min_height,
        max_rank=max_rank,
    )

    # ── Summary bar ───────────────────────────────────────────────────────────
    n_titles    = len(df)
    n_buildings = df["building_id"].nunique() if not df.empty else 0
    st.write(f"**{n_titles:,} titles** · **{n_buildings:,} buildings**")

    # ── Results table ─────────────────────────────────────────────────────────
    if df.empty:
        st.info("No titles match your current filters. Try loosening a filter in the sidebar.")
        return

    # Build display version — hide zero completion years, add SC link, rename columns
    df_display = df[[
        "title", "building_name", "city_name", "country_name",
        "height_architecture", "completed", "developer_owner", "architect", "building_id",
    ]].copy()
    df_display["completed"] = df_display["completed"].replace(0, pd.NA)
    # Build the Skyscraper Center URL for each building
    df_display["sc_url"] = (
        "https://www.skyscrapercenter.com/building/id/"
        + df_display["building_id"].astype(str)
    )
    df_display = df_display.drop(columns=["building_id"])
    df_display.columns = [
        "Title", "Building", "City", "Country",
        "Height (m)", "Year", "Developer / Owner", "Architect", "SC Link",
    ]

    event = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        height=420,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Height (m)": st.column_config.NumberColumn(format="%.0f m"),
            "Year":        st.column_config.NumberColumn(format="%d"),
            # Renders the URL as a clickable "View ↗" link in its own column
            "SC Link": st.column_config.LinkColumn(
                "Skyscraper Center",
                display_text="View ↗",
                help="Open this building on The Skyscraper Center",
            ),
        },
    )

    # ── Detail panel (appears below table when a row is clicked) ──────────────
    selected_rows = event.selection.rows if (event and hasattr(event, "selection")) else []
    if selected_rows:
        show_detail(
            row=df.iloc[selected_rows[0]],
            df_all=df_all,
            show_competitors=show_competitors,
        )


if __name__ == "__main__":
    main()
