import streamlit as st
import pandas as pd
from serpapi import GoogleSearch
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

# ────────────────────────────────────────────────────
# 1. Configuration
# ────────────────────────────────────────────────────
st.set_page_config(page_title="Vogels – Rich Snippets SERP", layout="wide")

try:
    SERPAPI_KEY = st.secrets["serpapi_key"]
except Exception:
    st.error("❌ Clé SerpApi manquante dans `.streamlit/secrets.toml`.")
    st.stop()

VOGELS_DOMAIN = "vogels.com"

DEFAULT_QUERIES = """vogels support TV mural
vogels TMS 1000
vogels MotionMount
vogels wall mount 65 inch
support mural TV orientable"""

# ────────────────────────────────────────────────────
# 2. Sidebar
# ────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://www.vogels.com/media/logo/stores/1/Vogels_Logo_RGB.svg", width=140)
    st.header("⚙️ Paramètres")
    hl = st.selectbox("Langue (hl)", ["fr", "en", "de", "nl", "es", "it"], index=1)
    gl = st.selectbox("Pays (gl)", ["fr", "us", "de", "nl", "gb", "es"], index=1)
    num_results = st.slider("Résultats organiques analysés", 10, 30, 10, step=10)
    max_workers = st.slider("Threads simultanés", 1, 8, 3)

    st.markdown("---")
    debug_mode = st.toggle(
        "🐛 Mode debug",
        value=False,
        help="Affiche la structure brute SerpApi pour inspecter les champs rich snippet",
    )

    st.markdown("---")
    st.markdown(
        "**Données extraites**\n\n"
        "- ⭐ Note moyenne (étoiles)\n"
        "- 💬 Nombre d'avis\n"
        "- 💶 Prix (si disponible)\n"
        "- 📍 Position organique\n"
        "- 🔗 URL du résultat"
    )

# ────────────────────────────────────────────────────
# 3. Zone de saisie des requêtes
# ────────────────────────────────────────────────────
st.title("⭐ Vogels – Rich Snippets dans la SERP Google")
st.markdown(
    "Analyse la présence de **données enrichies** (notes, avis, prix) pour `vogels.com` "
    "dans les résultats organiques Google via SerpApi."
)

queries_raw = st.text_area(
    "📋 Liste de requêtes (une par ligne)",
    value=DEFAULT_QUERIES,
    height=160,
)
queries = [q.strip() for q in queries_raw.splitlines() if q.strip()]
st.caption(f"**{len(queries)} requête(s)** chargée(s)")

# ────────────────────────────────────────────────────
# 4. Fonctions d'extraction
# ────────────────────────────────────────────────────

def fetch_raw(query: str, hl: str, gl: str, num: int) -> dict:
    params = {
        "q": query,
        "api_key": SERPAPI_KEY,
        "hl": hl,
        "gl": gl,
        "num": num,
        "engine": "google",
    }
    return GoogleSearch(params).get_dict()


def extract_rich_snippet(result: dict) -> dict:
    """
    Tente d'extraire note, avis et prix depuis toutes les structures
    connues de rich_snippet dans SerpApi.
    Retourne un dict {note, avis, prix}.
    """
    note = avis = prix = None

    # ── Structure principale : rich_snippet.top / bottom ──────────────
    for zone in ("top", "bottom"):
        rs = result.get("rich_snippet", {}).get(zone, {})
        if not rs:
            continue

        ext = rs.get("detected_extensions", {})
        if note is None:
            note = ext.get("rating") or ext.get("average_rating") or ext.get("note")
        if avis is None:
            avis = (
                ext.get("reviews")
                or ext.get("review_count")
                or ext.get("votes")
                or ext.get("ratings_count")
                or ext.get("user_ratings_total")
            )
        if prix is None:
            # Prix parfois dans les extensions
            prix = ext.get("price")

        # Prix également dans les items directs du rich_snippet
        if prix is None:
            for item in rs.get("extensions", []):
                if isinstance(item, str) and any(c in item for c in ("€", "$", "£", "USD", "EUR")):
                    prix = item
                    break

    # ── Fallback : detected_extensions au niveau résultat ─────────────
    direct_ext = result.get("detected_extensions", {})
    if note is None:
        note = direct_ext.get("rating") or direct_ext.get("average_rating")
    if avis is None:
        avis = direct_ext.get("reviews") or direct_ext.get("review_count")
    if prix is None:
        prix = direct_ext.get("price")

    # ── Fallback : structured_data ────────────────────────────────────
    for sd in result.get("rich_snippet_parsed", []) + result.get("structured_data", []):
        if isinstance(sd, dict):
            if note is None:
                note = (
                    sd.get("ratingValue")
                    or sd.get("rating")
                    or (sd.get("aggregateRating", {}) or {}).get("ratingValue")
                )
            if avis is None:
                avis = (
                    sd.get("reviewCount")
                    or sd.get("ratingCount")
                    or (sd.get("aggregateRating", {}) or {}).get("reviewCount")
                    or (sd.get("aggregateRating", {}) or {}).get("ratingCount")
                )
            if prix is None:
                offers = sd.get("offers", {}) or {}
                prix = offers.get("price") or sd.get("price")

    # ── Formatage ─────────────────────────────────────────────────────
    return {
        "Note": f"⭐ {note}" if note is not None else "—",
        "Avis": f"💬 {int(float(str(avis).replace(',', '').replace(' ', '')))}" if avis is not None else "—",
        "Prix": f"💶 {prix}" if prix is not None else "—",
        "_has_rich": note is not None or avis is not None or prix is not None,
    }


def extract_vogels_results(query: str, hl: str, gl: str, num: int) -> tuple[list[dict], dict]:
    try:
        data = fetch_raw(query, hl, gl, num)
    except Exception as exc:
        return [_row(query, "—", "⚠️ Erreur API", str(exc), "", "—", "—", "—", False)], {}

    rows = []
    organic = data.get("organic_results", [])

    for pos, result in enumerate(organic, start=1):
        link = result.get("link", "")
        if VOGELS_DOMAIN not in link:
            continue

        rs = extract_rich_snippet(result)
        rows.append(_row(
            query    = query,
            position = pos,
            titre    = result.get("title", "—"),
            url      = link,
            snippet  = result.get("snippet", "—"),
            note     = rs["Note"],
            avis     = rs["Avis"],
            prix     = rs["Prix"],
            has_rich = rs["_has_rich"],
        ))

    if not rows:
        rows.append(_row(query, "—", "❌ Absent", "Vogels absent des résultats analysés", "", "—", "—", "—", False))

    return rows, data


def _row(query, position, titre, url, snippet, note, avis, prix, has_rich) -> dict:
    return {
        "Requête"   : query,
        "Position"  : position,
        "Titre"     : titre,
        "URL"       : url,
        "Note"      : note,
        "Avis"      : avis,
        "Prix"      : prix,
        "Rich Snip.": "✅" if has_rich else "❌",
        "Snippet"   : snippet,
    }


@st.cache_data(ttl=3_600, show_spinner=False)
def run_all(queries_tuple: tuple, hl: str, gl: str, num: int, workers: int) -> tuple[pd.DataFrame, dict]:
    all_rows, all_raw = [], {}
    progress = st.progress(0.0, text="🔄 Analyse des SERP…")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(extract_vogels_results, q, hl, gl, num): q
            for q in queries_tuple
        }
        total = len(futures)
        for i, future in enumerate(as_completed(futures), 1):
            rows, raw = future.result()
            all_rows.extend(rows)
            all_raw[futures[future]] = raw
            progress.progress(i / total, text=f"🔄 {i}/{total} requêtes analysées…")
    progress.empty()
    return pd.DataFrame(all_rows), all_raw


# ────────────────────────────────────────────────────
# 5. Lancement + Affichage
# ────────────────────────────────────────────────────

if st.button("🚀 Lancer l'extraction", type="primary", disabled=len(queries) == 0):

    df, raw_data = run_all(tuple(queries), hl, gl, num_results, max_workers)

    # ── KPIs ─────────────────────────────────────────
    present   = df[df["Titre"] != "❌ Absent"]
    absent    = df[df["Titre"] == "❌ Absent"]
    with_rich = df[df["Rich Snip."] == "✅"]
    with_note = df[df["Note"] != "—"]
    with_prix = df[df["Prix"] != "—"]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Requêtes analysées",       len(queries))
    c2.metric("🔗 URLs Vogels détectées", len(present))
    c3.metric("⭐ Avec rich snippet",     len(with_rich))
    c4.metric("🌟 Avec note",             len(with_note))
    c5.metric("💶 Avec prix",             len(with_prix))

    # ── Taux de rich snippet ──────────────────────────
    if len(present) > 0:
        rate = round(len(with_rich) / len(present) * 100, 1)
        st.info(f"📊 **Taux de rich snippet** : {rate} % des URLs Vogels détectées affichent des données enrichies.")

    st.markdown("---")

    # ── Tableau principal ────────────────────────────
    st.subheader("📊 Résultats détaillés")

    rich_filter = st.radio(
        "Afficher",
        ["Tous", "Avec rich snippet ✅", "Sans rich snippet ❌"],
        horizontal=True,
    )
    df_filtered = df.copy()
    if rich_filter == "Avec rich snippet ✅":
        df_filtered = df[df["Rich Snip."] == "✅"]
    elif rich_filter == "Sans rich snippet ❌":
        df_filtered = df[df["Rich Snip."] == "❌"]

    st.dataframe(
        df_filtered,
        use_container_width=True,
        height=420,
        column_config={
            "URL": st.column_config.LinkColumn("URL", display_text="🔗 Voir"),
        },
        column_order=["Requête", "Position", "Rich Snip.", "Note", "Avis", "Prix", "Titre", "URL", "Snippet"],
    )

    # ── Vue par requête ──────────────────────────────
    st.markdown("---")
    st.subheader("🔍 Détail par requête")

    for query in df["Requête"].unique():
        subset = df[df["Requête"] == query]
        has_vp = any(subset["Titre"] != "❌ Absent")
        n_rich = len(subset[subset["Rich Snip."] == "✅"])
        label  = f"{'✅' if has_vp else '❌'} {query}" + (f" — ⭐ {n_rich} rich snippet(s)" if n_rich else "")

        with st.expander(label):
            if not has_vp:
                st.info("Vogels n'apparaît pas dans les résultats analysés pour cette requête.")
            else:
                for _, r in subset.iterrows():
                    col_pos, col_badge, col_note, col_avis, col_prix = st.columns([1, 1.2, 1.5, 1.5, 1.5])
                    col_pos.markdown(f"**Pos. `{r['Position']}`**")
                    col_badge.markdown(r["Rich Snip."])
                    col_note.markdown(r["Note"])
                    col_avis.markdown(r["Avis"])
                    col_prix.markdown(r["Prix"])
                    st.markdown(f"**{r['Titre']}** → [{r['URL']}]({r['URL']})")
                    if r["Snippet"] not in ("—", "", None):
                        st.caption(r["Snippet"])
                    st.markdown("---")

            # ── 🐛 MODE DEBUG ─────────────────────────────
            if debug_mode and query in raw_data:
                st.markdown("**🐛 Structure brute SerpApi — résultats Vogels uniquement**")

                vp_results = [
                    r for r in raw_data[query].get("organic_results", [])
                    if VOGELS_DOMAIN in r.get("link", "")
                ]

                if vp_results:
                    for r in vp_results:
                        st.markdown(f"**Position {r.get('position')} — Clés disponibles :**")
                        keys_info = {k: f"{type(v).__name__} → {str(v)[:200]}" for k, v in r.items()}
                        st.json(keys_info)
                        if "rich_snippet" in r:
                            st.markdown("**🎯 Clé `rich_snippet` (structure complète) :**")
                            st.json(r["rich_snippet"])
                        else:
                            st.warning("⚠️ Pas de clé `rich_snippet` pour ce résultat.")
                        if "detected_extensions" in r:
                            st.markdown("**🔍 Clé `detected_extensions` :**")
                            st.json(r["detected_extensions"])
                else:
                    st.info("Aucun résultat Vogels dans la réponse brute.")

    # ── Exports ──────────────────────────────────────
    st.markdown("---")
    col1, col2 = st.columns(2)

    csv = df.to_csv(index=False).encode("utf-8")
    col1.download_button(
        "💾 Télécharger CSV",
        data=csv,
        file_name="vogels_rich_snippets.csv",
        mime="text/csv",
    )

    xlsx_buffer = BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Détail")
        summary = (
            df[df["Rich Snip."] == "✅"][["Requête", "Position", "Note", "Avis", "Prix", "URL"]]
            .reset_index(drop=True)
        )
        summary.to_excel(writer, index=False, sheet_name="Rich Snippets")
    xlsx_buffer.seek(0)
    col2.download_button(
        "📊 Télécharger XLSX",
        data=xlsx_buffer,
        file_name="vogels_rich_snippets.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.format",
    )
