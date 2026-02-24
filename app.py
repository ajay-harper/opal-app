"""
Opal V3 Direct — COI Generator

Upload insurance PDFs → classify → extract (2 Claude calls per file) →
edit fields → generate filled ACORD PDF → download.

No reconciliation, no NAIC enrichment, no SOP lookup.
Just raw Claude output via the Anthropic Python SDK.

Usage:
    cd opal_v3/opal-app
    streamlit run app.py
"""

import anthropic
import base64
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import streamlit as st

# ── Load .env ────────────────────────────────────────────────────────
for env_path in [Path(__file__).resolve().parent / ".env", Path(__file__).resolve().parent.parent / ".env", Path.home() / ".env"]:
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# Add opal_v2 to path for acord_filler + forms
OPAL_V2_DIR = Path(__file__).resolve().parent.parent.parent / "opal_v2"
sys.path.insert(0, str(OPAL_V2_DIR))

from acord_filler import fill_pdf, ACORD25_FIELDS, ACORD27_FIELDS, ACORD30_FIELDS, determine_forms

# opal-test for anvil mapper
OPAL_TEST_DIR = Path(__file__).resolve().parent.parent.parent / "opal-test"
sys.path.insert(0, str(OPAL_TEST_DIR))

from anvil_mapper import opal_to_anvil, fill_anvil_pdf

FORM_PATHS = {
    "25": str(OPAL_V2_DIR / "forms" / "acord25.pdf"),
    "27": str(OPAL_V2_DIR / "forms" / "acord27.pdf"),
    "30": str(OPAL_V2_DIR / "forms" / "acord30.pdf"),
}

MODEL = "claude-opus-4-6"

# ── Prompts ──────────────────────────────────────────────────────────

CLASSIFY_PROMPT = """You are an insurance document classifier. For the given PDF, determine the document type.

Respond with ONLY a JSON object:
{"doc_type": "<type>", "confidence": <0.0-1.0>}

Valid types: binder, policy_declaration, endorsement, prior_coi, confirmation, quote, email_only

Key signals:
- "Binder" / "This confirms binding" -> binder
- "Declarations" / "Dec Page" -> policy_declaration
- "Endorsement" / "Amendment" / "Rider" -> endorsement
- "Certificate of Insurance" / ACORD form -> prior_coi
- "Quote" / "Indication" / "Proposal" -> quote"""

EXTRACT_PROMPT = """You are an insurance binder data extraction specialist. Extract structured JSON from binder documents to populate ACORD forms.

## TASK

1. Read the binder and identify coverage types (GL, Property, Auto, Workers Comp, Umbrella/Excess, Garage).
2. Determine applicable ACORD forms:
   - **ACORD 25** — GL, Auto, Umbrella, Workers Comp
   - **ACORD 27** — Property (single location, simple)
   - **ACORD 28** — Property (detailed/multi-location)
   - **ACORD 30** — Garage liability (auto dealers, repair shops, parking garages)
   Set inapplicable forms to `null`.
3. Extract all explicitly present data. Leave missing fields as `""`, `null`, or `false`.
4. If a limit is explicitly EXCLUDED on the binder (e.g., "Excluded", "Not Covered",
   "N/A"), set the value to the string `"Excluded"` — not `null`, not `0`.

## DEFAULTS & AUTO-POPULATION

- **producer.name** → Always `"Harper Global Enterprises Inc."`
- **producer.contactName** → Always `"Dakotah Rice"`
- **producer.phone** → Always `"470-839-4314"`
- **producer.fax** → Always `""`
- **producer.email** → Always `"service@harperinsure.com"`
- **producer.address** → Always `"1035 Rockingham Street, Alpharetta, GA 30022"`
- **certificateHolder** → Copy the `insured.name` and `insured.address` into the certificate holder fields automatically.
- **descriptionOfOperations** → Always set to `""`. Do not populate this field.
- **NAIC numbers** → Leave `naic` as `""` for all carriers. NAIC lookup is handled separately.
- **Endorsements** → If an endorsement value is "No", "N/A", or not present, **omit it entirely** from the output. Only include endorsements that are explicitly confirmed as included/true.

## JSON TEMPLATE — YOU MUST USE THIS EXACT STRUCTURE

```json
{
  "_notes": [],
  "producer": {
    "name": "Harper Global Enterprises Inc.",
    "contactName": "Dakotah Rice",
    "phone": "470-839-4314",
    "fax": "",
    "email": "service@harperinsure.com",
    "address": "1035 Rockingham Street, Alpharetta, GA 30022"
  },
  "insured": {
    "name": "",
    "address": ""
  },
  "carriers": [
    { "letter": "A", "name": "", "naic": "" }
  ],
  "acord25": {
    "certificateNumber": "",
    "gl": {
      "insurerLetter": "",
      "policyNumber": "",
      "effectiveDate": "",
      "expirationDate": "",
      "claimsMade": false,
      "occurrence": false,
      "limits": {
        "eachOccurrence": null,
        "damageToRentedPremises": null,
        "medicalExpense": null,
        "personalAdvInjury": null,
        "generalAggregate": null,
        "productsCompOp": null
      }
    },
    "auto": {
      "insurerLetter": "",
      "policyNumber": "",
      "effectiveDate": "",
      "expirationDate": "",
      "autoType": "",
      "combinedSingleLimit": null
    },
    "umbrella": {
      "insurerLetter": "",
      "policyNumber": "",
      "effectiveDate": "",
      "expirationDate": "",
      "type": "",
      "eachOccurrence": null,
      "aggregate": null,
      "retention": null
    },
    "workersComp": {
      "insurerLetter": "",
      "policyNumber": "",
      "effectiveDate": "",
      "expirationDate": "",
      "eachAccident": null,
      "diseasePolicyLimit": null,
      "diseaseEachEmployee": null
    },
    "descriptionOfOperations": "",
    "certificateHolder": { "name": "", "address": "" },
    "endorsements": {}
  },
  "acord27": null,
  "acord28": null,
  "acord30": {
    "garageLiability": {
      "insurerLetter": "",
      "policyNumber": "",
      "effectiveDate": "",
      "expirationDate": "",
      "premisesOperations": null,
      "autoOnlyEachAccident": null,
      "otherThanAutoOnly": null,
      "autoOnlyAggregate": null,
      "eachOccurrence": null,
      "aggregate": null
    },
    "garagekeepersLegal": {
      "comprehensive": null,
      "collision": null,
      "specifiedPerils": null
    },
    "certificateHolder": { "name": "", "address": "" },
    "descriptionOfOperations": "",
    "endorsements": {}
  }
}
```

## EXTRACTION RULES

### Producer vs Wholesaler
- **Producer** = retail agent/broker. Goes on the ACORD form.
- **Wholesaler** (RT Specialty, AmWINS, CRC Group) = intermediary, NOT the producer.

### Formatting
- Dates: `MM/DD/YYYY`. Dollar amounts: plain numbers (e.g., `1000000`).

### Address Selection
Prefer mailing address from carrier binder/dec page over confirmation pages.

### Claims-Made vs Occurrence
Standard ISO CGL (CG 00 01) = occurrence. If form says "Claims-Made" or has retro date = claims-made.
If there is any Commercial General Liability coverage at all, never leave both flags false:
- set `claimsMade=true` for claims-made CGL
- otherwise set `occurrence=true` (default)

### Carriers
- Each carrier gets a letter (A, B, C, ...) and entry in `carriers` array.
- `insurerLetter` in each coverage section must reference a carrier letter.

### Umbrella / Excess — FALSE POSITIVE GUARD
Set `acord25.umbrella` to `null` UNLESS ALL THREE:
1. A **separate** umbrella/excess policy number (different from GL)
2. An umbrella occurrence limit (dollar amount)
3. The coverage is clearly bound
When in doubt, set null.

### Garage Policies (ACORD 30)
- Garage policies combine GL and Auto under a single policy.
- Use `acord30` for garage-specific fields (garage liability, garagekeepers).
- Map the GL portion to `acord25.gl` as well for the ACORD 25 certificate.
- Do NOT populate `acord25.auto` separately for garage policies.

### Products/Completed Operations Aggregate
If "Included" (not a dollar amount), set `productsCompOp` to `"Included"`.

### D&O / Management Liability / Professional Liability
NOT General Liability. Do NOT map to `acord25.gl`. Note in `_notes`.

### Description of Operations
ALWAYS set `descriptionOfOperations` to `""`. Never populate from document.

### Endorsements
- Only include if confirmed on the BOUND policy.
- If from application/quote only, do NOT include.
- If "No", "N/A", or absent → omit.

## OUTPUT

Return ONLY the raw JSON object. No markdown fences, no surrounding text."""


# ── Claude helpers ───────────────────────────────────────────────────

def get_client():
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    return anthropic.Anthropic(api_key=key)


def strip_fences(text):
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t[3:]
    if t.endswith("```"):
        t = t[:-3]
    return t.strip()


def call_claude(client, system, user_content, max_tokens=8192):
    resp = client.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_content}],
    )
    return strip_fences(resp.content[0].text)


# ── Page config ──────────────────────────────────────────────────────

st.set_page_config(
    page_title="Opal V3 Direct — COI Generator",
    page_icon="📋",
    layout="wide",
)

# ── Load env ─────────────────────────────────────────────────────────
for env_path in [Path(__file__).parent / ".env", OPAL_V2_DIR / ".env"]:
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main():
    st.title("Opal V3 Direct — COI Generator")
    st.caption("Direct Claude extraction (2 calls per file) — no reconciliation, no NAIC enrichment")

    # ── Sidebar ──────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Settings")
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            api_key = st.text_input("Anthropic API Key", type="password")
            if api_key:
                os.environ["ANTHROPIC_API_KEY"] = api_key
        else:
            st.success("API key loaded from .env")

        st.divider()
        st.header("Pipeline Status")
        if "elapsed" in st.session_state:
            st.metric("Last extraction", f"{st.session_state['elapsed']:.1f}s")
        if "classifications" in st.session_state:
            st.metric("Files processed", len(st.session_state["classifications"]))

    # ── Section 1: Upload Documents ──────────────────────────────────
    st.header("1. Upload Documents")
    uploaded_files = st.file_uploader(
        "Upload binders, quotes, applications, endorsements, or any insurance PDFs",
        type=["pdf"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        st.write(f"**{len(uploaded_files)} file(s) staged:**")
        for f in uploaded_files:
            st.write(f"- {f.name} ({f.size // 1024}KB)")

        with st.expander("Preview uploaded PDFs", expanded=False):
            if len(uploaded_files) == 1:
                pdf_bytes = uploaded_files[0].read()
                uploaded_files[0].seek(0)
                b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                st.markdown(
                    f'<iframe src="data:application/pdf;base64,{b64}" '
                    f'width="100%" height="600px" style="border: 1px solid #e5e7eb; border-radius: 8px;"></iframe>',
                    unsafe_allow_html=True,
                )
            else:
                preview_tabs = st.tabs([f.name for f in uploaded_files])
                for tab, f in zip(preview_tabs, uploaded_files):
                    with tab:
                        pdf_bytes = f.read()
                        f.seek(0)
                        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                        st.markdown(
                            f'<iframe src="data:application/pdf;base64,{b64}" '
                            f'width="100%" height="600px" style="border: 1px solid #e5e7eb; border-radius: 8px;"></iframe>',
                            unsafe_allow_html=True,
                        )

    # ── Extract Button ───────────────────────────────────────────────
    col1, col2 = st.columns([1, 3])
    with col1:
        extract_btn = st.button(
            "🔍 Extract",
            type="primary",
            disabled=not uploaded_files or not os.environ.get("ANTHROPIC_API_KEY"),
        )

    # ── Run Extraction ───────────────────────────────────────────────
    if extract_btn and uploaded_files:
        client = get_client()
        if not client:
            st.error("ANTHROPIC_API_KEY not set")
            st.stop()

        files = []
        for f in uploaded_files:
            b64 = base64.standard_b64encode(f.read()).decode("utf-8")
            files.append({"filename": f.name, "base64": b64})

        with st.spinner("Running direct Claude extraction..."):
            progress = st.progress(0, text="Starting extraction...")
            start = time.time()

            all_classifications = []
            all_extractions = []

            for i, f in enumerate(files):
                pct_base = int((i / len(files)) * 100)

                progress.progress(pct_base + 5, text=f"Classifying {f['filename']}...")
                try:
                    raw = call_claude(client, CLASSIFY_PROMPT, [
                        {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": f["base64"]}},
                        {"type": "text", "text": "Classify this insurance document."},
                    ], max_tokens=512)
                    parsed = json.loads(raw)
                    doc_type = parsed.get("doc_type", "unknown")
                    confidence = parsed.get("confidence", 0)
                except Exception as e:
                    st.warning(f"Classification failed for {f['filename']}: {e}")
                    doc_type, confidence = "unknown", 0

                all_classifications.append({"filename": f["filename"], "doc_type": doc_type, "confidence": confidence})

                progress.progress(pct_base + 50, text=f"Extracting {f['filename']} ({doc_type})...")
                try:
                    raw = call_claude(client, EXTRACT_PROMPT, [
                        {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": f["base64"]}},
                        {"type": "text", "text": f"This is a {doc_type} document. Extract all data into the JSON template."},
                    ])
                    extraction = json.loads(raw)
                    all_extractions.append(extraction)
                except Exception as e:
                    st.warning(f"Extraction failed for {f['filename']}: {e}")
                    all_extractions.append({})

            elapsed = time.time() - start
            progress.progress(100, text=f"Done in {elapsed:.1f}s")

            primary = all_extractions[0] if all_extractions else {}

            st.session_state["extraction_result"] = primary
            st.session_state["classifications"] = all_classifications
            st.session_state["elapsed"] = elapsed

    # ── Section 2: Extraction Results ────────────────────────────────
    if "extraction_result" in st.session_state:
        result = st.session_state["extraction_result"]
        classifications = st.session_state.get("classifications", [])

        st.header("2. Extraction Results")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            insured = result.get("insured", {}).get("name", "Unknown")
            st.metric("Insured", insured)
        with col2:
            carriers = result.get("carriers", [])
            st.metric("Carriers", len(carriers))
        with col3:
            forms = determine_forms(result)
            st.metric("Forms", ", ".join(f"ACORD {f}" for f in forms) or "None")
        with col4:
            st.metric("Time", f"{st.session_state.get('elapsed', 0):.1f}s")

        with st.expander("Document Classifications", expanded=False):
            for c in classifications:
                conf_pct = int(c["confidence"] * 100)
                st.write(f"**{c['filename']}** → `{c['doc_type']}` ({conf_pct}%)")

        notes = result.get("_notes", [])
        if notes:
            with st.expander(f"Notes ({len(notes)})", expanded=False):
                for n in notes:
                    st.write(f"- {n}")

        # ── Section 3: Edit Extraction ───────────────────────────────
        st.header("3. Edit Extraction")
        st.caption("Modify any field before generating the PDF")

        tabs = st.tabs(["Producer & Insured", "Carriers", "GL Coverage", "Auto", "Umbrella", "Workers Comp", "Certificate Holder", "Endorsements", "Raw JSON"])

        with tabs[0]:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Producer")
                producer = result.setdefault("producer", {})
                producer["name"] = st.text_input("Producer Name", producer.get("name", ""))
                producer["contactName"] = st.text_input("Contact", producer.get("contactName", ""))
                producer["phone"] = st.text_input("Phone", producer.get("phone", ""))
                producer["email"] = st.text_input("Email", producer.get("email", ""))
                producer["address"] = st.text_input("Address", producer.get("address", ""))
            with col2:
                st.subheader("Insured")
                insured_obj = result.setdefault("insured", {})
                insured_obj["name"] = st.text_input("Insured Name", insured_obj.get("name", ""))
                insured_obj["address"] = st.text_input("Insured Address", insured_obj.get("address", ""))

        with tabs[1]:
            carriers = result.get("carriers", [])
            for i, c in enumerate(carriers):
                col1, col2, col3 = st.columns([1, 4, 2])
                with col1:
                    c["letter"] = st.text_input(f"Letter #{i+1}", c.get("letter", ""), key=f"cl_{i}")
                with col2:
                    c["name"] = st.text_input(f"Carrier #{i+1}", c.get("name", ""), key=f"cn_{i}")
                with col3:
                    c["naic"] = st.text_input(f"NAIC #{i+1}", c.get("naic", ""), key=f"cnaic_{i}")

        with tabs[2]:
            acord25 = result.get("acord25") or {}
            gl = acord25.get("gl") or {}
            if gl:
                col1, col2 = st.columns(2)
                with col1:
                    gl["insurerLetter"] = st.text_input("Insurer Letter", gl.get("insurerLetter", ""))
                    gl["policyNumber"] = st.text_input("Policy Number", gl.get("policyNumber", ""))
                    gl["effectiveDate"] = st.text_input("Effective Date", gl.get("effectiveDate", ""))
                    gl["expirationDate"] = st.text_input("Expiration Date", gl.get("expirationDate", ""))
                    gl["occurrence"] = st.checkbox("Occurrence", gl.get("occurrence", False))
                    gl["claimsMade"] = st.checkbox("Claims Made", gl.get("claimsMade", False))
                with col2:
                    limits = gl.setdefault("limits", {})
                    for k in ["eachOccurrence", "damageToRentedPremises", "medicalExpense", "personalAdvInjury", "generalAggregate", "productsCompOp"]:
                        label = k.replace("_", " ").title()
                        val = limits.get(k)
                        display = str(val) if val is not None else ""
                        new_val = st.text_input(label, display, key=f"gl_{k}")
                        if new_val == "" or new_val.lower() == "none":
                            limits[k] = None
                        elif new_val.lower() == "excluded":
                            limits[k] = "Excluded"
                        else:
                            try:
                                limits[k] = int(new_val.replace(",", ""))
                            except ValueError:
                                limits[k] = new_val
                acord25["gl"] = gl
            else:
                st.info("No GL coverage extracted")

        with tabs[3]:
            auto = acord25.get("auto")
            if auto and isinstance(auto, dict):
                auto["policyNumber"] = st.text_input("Auto Policy Number", auto.get("policyNumber", ""))
                auto["effectiveDate"] = st.text_input("Auto Effective", auto.get("effectiveDate", ""))
                auto["expirationDate"] = st.text_input("Auto Expiration", auto.get("expirationDate", ""))
                val = st.text_input("Combined Single Limit", str(auto.get("combinedSingleLimit", "")))
                try:
                    auto["combinedSingleLimit"] = int(val.replace(",", "")) if val else None
                except ValueError:
                    pass
            else:
                st.info("No auto coverage extracted")

        with tabs[4]:
            umb = acord25.get("umbrella")
            if umb and isinstance(umb, dict):
                umb["policyNumber"] = st.text_input("Umbrella Policy Number", umb.get("policyNumber", ""))
                val = st.text_input("Umbrella Each Occurrence", str(umb.get("eachOccurrence", "")))
                try:
                    umb["eachOccurrence"] = int(val.replace(",", "")) if val else None
                except ValueError:
                    pass
            else:
                st.info("No umbrella coverage extracted (this is usually correct)")

        with tabs[5]:
            wc = acord25.get("workersComp")
            if wc and isinstance(wc, dict):
                wc["policyNumber"] = st.text_input("WC Policy Number", wc.get("policyNumber", ""))
                wc["effectiveDate"] = st.text_input("WC Effective", wc.get("effectiveDate", ""))
                wc["expirationDate"] = st.text_input("WC Expiration", wc.get("expirationDate", ""))
                for k in ["eachAccident", "diseasePolicyLimit", "diseaseEachEmployee"]:
                    val = st.text_input(k.replace("_", " ").title(), str(wc.get(k, "")), key=f"wc_{k}")
                    try:
                        wc[k] = int(val.replace(",", "")) if val else None
                    except ValueError:
                        pass
            else:
                st.info("No workers comp coverage extracted")

        with tabs[6]:
            ch = acord25.get("certificateHolder", {})
            ch["name"] = st.text_input("Cert Holder Name", ch.get("name", ""))
            ch["address"] = st.text_input("Cert Holder Address", ch.get("address", ""))
            acord25["certificateHolder"] = ch

        with tabs[7]:
            endorsements = acord25.get("endorsements", {})
            endorsements["additionalInsured"] = st.checkbox("Additional Insured", endorsements.get("additionalInsured", False))
            endorsements["waiverOfSubrogation"] = st.checkbox("Waiver of Subrogation", endorsements.get("waiverOfSubrogation", False))
            endorsements["primaryNonContributory"] = st.checkbox("Primary & Non-Contributory", endorsements.get("primaryNonContributory", False))
            acord25["endorsements"] = endorsements

        with tabs[8]:
            clean = {k: v for k, v in result.items() if not k.startswith("_")}
            st.json(clean)

        result["acord25"] = acord25
        st.session_state["extraction_result"] = result

        # ── Section 4: Generate ACORD PDF ────────────────────────────
        st.header("4. Generate ACORD PDF")

        forms_to_generate = determine_forms(result)
        forms_to_generate = [f.replace("ACORD ", "") for f in forms_to_generate]
        valid_options = ["25", "27", "28", "30"]
        forms_to_generate = [f for f in forms_to_generate if f in valid_options]

        selected_forms = st.multiselect(
            "Select forms to generate",
            options=valid_options,
            default=forms_to_generate or ["25"],
        )

        if st.button("📄 Generate PDF", type="primary"):
            with st.spinner("Generating ACORD PDF(s)..."):
                generated = []
                form_configs = {
                    "25": ("ACORD 25", ACORD25_FIELDS),
                    "27": ("ACORD 27", ACORD27_FIELDS),
                    "30": ("ACORD 30", ACORD30_FIELDS),
                }

                for form_num in selected_forms:
                    template = Path(FORM_PATHS.get(form_num, ""))
                    if not template.exists():
                        st.warning(f"Template not found: {template}")
                        continue

                    if form_num not in form_configs:
                        st.warning(f"ACORD {form_num} field mapping not available")
                        continue

                    label, field_map = form_configs[form_num]

                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                        output_path = tmp.name

                    fill_pdf(str(template), output_path, field_map, result)

                    pdf_bytes = Path(output_path).read_bytes()
                    generated.append((form_num, label, pdf_bytes))

                for form_num, label, pdf_bytes in generated:
                    insured_name = result.get("insured", {}).get("name", "Unknown").replace(" ", "_")
                    filename = f"acord{form_num}_{insured_name}.pdf"

                    st.download_button(
                        f"⬇️ Download {label}",
                        data=pdf_bytes,
                        file_name=filename,
                        mime="application/pdf",
                        key=f"dl_{form_num}",
                    )

                    b64_pdf = base64.b64encode(pdf_bytes).decode("utf-8")
                    st.markdown(
                        f'<iframe src="data:application/pdf;base64,{b64_pdf}" '
                        f'width="100%" height="800" type="application/pdf"></iframe>',
                        unsafe_allow_html=True,
                    )

                if generated:
                    st.success(f"Generated {len(generated)} ACORD form(s)")

        # ── Section 5: Export via Anvil ───────────────────────────────
        st.divider()
        st.header("5. Export via Anvil")

        anvil_api_key = os.environ.get("ANVIL_API_KEY", "EVrpTiOBEL61BwJ6BmGOrggB0NBnDtPw")
        anvil_template_eid = os.environ.get("ANVIL_TEMPLATE_EID", "5ONfXsEAZgliFdPjR0mA")

        clean = {k: v for k, v in result.items() if not k.startswith("_")}
        clean["_description"] = result.get("_description", "")
        default_anvil = opal_to_anvil(clean)

        st.subheader("Edit Anvil Document Fields")
        st.caption("Review and modify any field before generating the PDF. Changes here go directly to the ACORD form.")

        edited_anvil_str = st.text_area(
            "Anvil Payload (JSON — edit any field)",
            value=json.dumps(default_anvil, indent=2),
            height=400,
            key="anvil_editor",
        )

        try:
            edited_anvil = json.loads(edited_anvil_str)
            payload_valid = True
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")
            payload_valid = False
            edited_anvil = default_anvil

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔨 Generate via Anvil", type="primary", disabled=not payload_valid):
                with st.spinner("Calling Anvil API..."):
                    try:
                        pdf_bytes = fill_anvil_pdf(
                            edited_anvil,
                            api_key=anvil_api_key,
                            template_eid=anvil_template_eid,
                        )

                        insured_name = result.get("insured", {}).get("name", "Unknown").replace(" ", "_")
                        filename = f"acord25_anvil_{insured_name}.pdf"

                        st.download_button(
                            "⬇️ Download Anvil ACORD 25",
                            data=pdf_bytes,
                            file_name=filename,
                            mime="application/pdf",
                            key="dl_anvil",
                        )

                        b64_pdf = base64.b64encode(pdf_bytes).decode("utf-8")
                        st.markdown(
                            f'<iframe src="data:application/pdf;base64,{b64_pdf}" '
                            f'width="100%" height="800" type="application/pdf"></iframe>',
                            unsafe_allow_html=True,
                        )

                        st.success("Anvil PDF generated successfully")

                    except Exception as e:
                        st.error(f"Anvil export failed: {e}")
                        import traceback
                        st.code(traceback.format_exc())

        with col2:
            if st.button("↩️ Reset to Extraction"):
                st.session_state["anvil_editor"] = json.dumps(default_anvil, indent=2)
                st.rerun()


if __name__ == "__main__":
    main()
