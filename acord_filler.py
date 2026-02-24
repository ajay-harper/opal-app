"""
ACORD PDF Form Filler Pipeline
Takes extracted binder JSON and fills ACORD 25, 27, and/or 28 PDFs.
"""

import json
import sys
import os
from datetime import datetime
from pathlib import Path
from pypdf import PdfReader, PdfWriter
from pypdf.generic import NameObject, BooleanObject, TextStringObject

# ── NAIC Code Dictionary ─────────────────────────────────────────────
NAIC_CODES = {
    "Allstate Fire and Casualty Insurance Company": "29688",
    "Allstate Indemnity Company": "19240",
    "American Family Insurance Company": "19275",
    "Amguard Insurance Company": "42390",
    "Auto-Owners Insurance Company": "18988",
    "Berkley Insurance Company": "32603",
    "Berkshire Hathaway Homestate Insurance Company": "20044",
    "Century Surety Company": "36951",
    "Church Mutual Insurance Company": "18767",
    "Cincinnati Insurance Company": "10677",
    "Continental Casualty Company": "20443",
    "Employers Mutual Casualty Company": "21415",
    "Erie Insurance Exchange": "26263",
    "Frankenmuth Mutual Insurance Company": "13986",
    "General Casualty Company of Wisconsin": "24414",
    "Great American Insurance Company": "16691",
    "Grinnell Mutual Reinsurance Company": "23973",
    "Hanover Insurance Company": "22292",
    "Hartford Fire Insurance Company": "19682",
    "Insurance Company of the West": "27847",
    "Indiana Lumbermens Mutual Insurance Company": "14265",
    "Liberty Mutual Fire Insurance Company": "23035",
    "Liberty Mutual Insurance Company": "23043",
    "Markel American Insurance Company": "28932",
    "Mesa Underwriters Specialty Insurance Company": "36838",
    "Merchants Bonding Company": "14494",
    "Nationwide Mutual Fire Insurance Company": "23779",
    "Nationwide Mutual Insurance Company": "23787",
    "Ohio Casualty Insurance Company": "24074",
    "Pennsylvania Lumbermens Mutual Insurance Company": "14974",
    "Philadelphia Indemnity Insurance Company": "18058",
    "Pinnacol Assurance": "41190",
    "Progressive Casualty Insurance Company": "24260",
    "Selective Insurance Company of America": "12572",
    "Sentry Insurance A Mutual Company": "24988",
    "Society Insurance": "14117",
    "State Auto Mutual Automobile Insurance Company": "25127",
    "State Farm Fire and Casualty Company": "25143",
    "State Farm General Insurance Company": "25151",
    "State Farm Mutual Automobile Insurance Company": "25178",
    "The Travelers Indemnity Company": "25658",
    "Travelers Casualty and Surety Company": "19046",
    "Travelers Indemnity Company of America": "25666",
    "USAA Casualty Insurance Company": "25968",
    "United States Liability Insurance Company": "25895",
    "West Bend Mutual Insurance Company": "15350",
    "Westfield Insurance Company": "24112",
    "Zurich American Insurance Company": "16535",
    "Accelerant Specialty Insurance Company": "16890",
    "Acceptance Indemnity Insurance Company": "20010",
    "Admiral Insurance Company": "24856",
    "Allied World Insurance Company": "22730",
    "Arch Insurance Company": "11150",
    "Ategrity Specialty Insurance Company": "16427",
    "Atain Specialty Insurance Company": "17159",
    "Berkley Assurance Company": "39462",
    "Canopius US Insurance Inc": "15692",
    "Capitol Indemnity Corporation": "10472",
    "Chubb Custom Insurance Company": "21784",
    "Colony Insurance Company": "39993",
    "Crum and Forster Specialty Insurance Company": "44520",
    "Evanston Insurance Company": "35378",
    "Everest Indemnity Insurance Company": "10851",
    "General Security Indemnity Company of Arizona": "15865",
    "General Star Indemnity Company": "37362",
    "Gotham Insurance Company": "25569",
    "Hamilton Select Insurance Inc": "17178",
    "Great American E&S Insurance Company": "26344",
    "Hallmark Specialty Insurance Company": "44768",
    "Houston Casualty Company": "42374",
    "Illinois Union Insurance Company": "27960",
    "Indian Harbor Insurance Company": "36940",
    "James River Insurance Company": "12203",
    "Kinsale Insurance Company": "38920",
    "Landmark American Insurance Company": "33138",
    "Lexington Insurance Company": "19437",
    "Markel Insurance Company": "38970",
    "Maxum Indemnity Company": "26743",
    "Mt Hawley Insurance Company": "22306",
    "National Fire and Marine Insurance Company": "20079",
    "Nautilus Insurance Company": "17370",
    "Nonprofits Insurance Alliance of California": "10023",
    "North American Capacity Insurance Company": "43575",
    "Old Republic Union Insurance Company": "24147",
    "Prime Insurance Company": "17809",
    "QBE Specialty Insurance Company": "10219",
    "RLI Insurance Company": "13056",
    "Safety Specialty Insurance Company": "39012",
    "Scottsdale Insurance Company": "41297",
    "Seneca Insurance Company": "10936",
    "Starr Surplus Lines Insurance Company": "13604",
    "State National Insurance Company": "12831",
    "Steadfast Insurance Company": "26387",
    "Sutton Specialty Insurance Company": "16848",
    "SiriusPoint Specialty Insurance Corporation": "16820",
    "Third Coast Insurance Company": "10713",
    "Trisura Specialty Insurance Company": "16188",
    "Tudor Insurance Company": "37982",
    "United National Insurance Company": "13064",
    "Westchester Surplus Lines Insurance Company": "10172",
    "XL Specialty Insurance Company": "37885",
    "Starstone Specialty Insurance Company": "44776",
}

# ── Configuration ───────────────────────────────────────────────────
FORM_PATHS = {
    "25": "forms/acord25.pdf",
    "27": "forms/acord27.pdf",
    "28": "forms/acord28.pdf",
    "30": "forms/acord30.pdf",
}


# ── Address Parser ──────────────────────────────────────────────────
def _parse_address(addr: str) -> dict:
    if not addr:
        return {"line1": "", "line2": "", "city": "", "state": "", "zip": ""}
    addr = addr.replace("\n", ", ").replace("\r", "")
    parts = [p.strip() for p in addr.split(",")]
    r = {"line1": "", "line2": "", "city": "", "state": "", "zip": ""}
    if len(parts) >= 3:
        last = parts[-1].strip().split()
        if len(last) >= 2 and (last[-1].isdigit() or len(last[-1]) >= 5):
            r["state"], r["zip"] = last[0], last[-1]
        elif len(last) == 1 and last[0].isdigit():
            r["zip"] = last[0]
        else:
            r["state"] = parts[-1].strip()
        r["city"] = parts[-2].strip()
        r["line1"] = parts[0].strip()
        if len(parts) > 3:
            r["line2"] = ", ".join(parts[1:-2]).strip()
    elif len(parts) == 2:
        r["line1"] = parts[0].strip()
        last = parts[1].strip().split()
        if len(last) >= 3:
            r["zip"], r["state"] = last[-1], last[-2]
            r["city"] = " ".join(last[:-2])
        else:
            r["city"] = parts[1].strip()
    else:
        r["line1"] = addr
    return r

def _al1(a): return _parse_address(a)["line1"]
def _al2(a): return _parse_address(a)["line2"]
def _aci(a): return _parse_address(a)["city"]
def _ast(a): return _parse_address(a)["state"]
def _azp(a): return _parse_address(a)["zip"]


# ── Money Formatter ─────────────────────────────────────────────────
def _fm(val):
    if val is None or val == "":
        return ""
    # If the extraction explicitly says "Excluded"
    if isinstance(val, str) and val.lower() in ("excluded", "excl", "n/a"):
        return "Excluded"
    try:
        n = float(val)
        # 0 means the coverage is excluded, not a $0 limit
        if n == 0:
            return "Excluded"
        return f"{int(n):,}" if n == int(n) else f"{n:,.2f}"
    except (ValueError, TypeError):
        return str(val)


# ── Data Accessors ──────────────────────────────────────────────────
def _carrier(d, letter):
    for c in d.get("carriers", []):
        if c.get("letter", "").upper() == letter.upper():
            return c.get("name", "")
    return ""

def _carrier_naic(d, letter):
    for c in d.get("carriers", []):
        if c.get("letter", "").upper() == letter.upper():
            naic = c.get("naic", "")
            if naic:
                return naic
            name = c.get("name", "")
            return NAIC_CODES.get(name, "")
    return ""

# ACORD 25 accessors
def _a25(d, k): return (d.get("acord25") or {}).get(k, "")
def _a25e(d, k): return (d.get("acord25") or {}).get("endorsements", {}).get(k, False)
def _gl(d, k): return (d.get("acord25") or {}).get("gl", {}).get(k, "")
def _glL(d, k): return _fm((d.get("acord25") or {}).get("gl", {}).get("limits", {}).get(k))
def _au(d, k):
    v = (d.get("acord25") or {}).get("auto", {}).get(k, "")
    return _fm(v) if isinstance(v, (int, float)) else v
def _um(d, k):
    v = (d.get("acord25") or {}).get("umbrella", {}).get(k, "")
    return _fm(v) if isinstance(v, (int, float)) else v
def _wc(d, k):
    v = (d.get("acord25") or {}).get("workersComp", {}).get(k, "")
    return _fm(v) if isinstance(v, (int, float)) else v

# Coverage existence helpers
def _has_gl(d):
    return bool(_gl(d, "policyNumber") or _glL(d, "eachOccurrence") or _glL(d, "generalAggregate"))

def _has_auto(d):
    return bool(_au(d, "policyNumber") or _au(d, "combinedSingleLimit"))

def _has_umbrella(d):
    return bool(_um(d, "policyNumber") or _um(d, "eachOccurrence"))

def _has_wc(d):
    return bool(_wc(d, "policyNumber") or _wc(d, "eachAccident"))

# ACORD 27 accessors
def _a27(d, k): return (d.get("acord27") or {}).get(k, "")
def _a27c(d, k): return (d.get("acord27") or {}).get("coverages", {}).get(k)
def _a27m(d, k): return (d.get("acord27") or {}).get("mortgageholder", {}).get(k, "")

# ACORD 28 accessors
def _a28(d, k): return (d.get("acord28") or {}).get(k, "")
def _a28p(d, i, k):
    prems = (d.get("acord28") or {}).get("premises", [])
    return prems[i].get(k, "") if i < len(prems) else ""
def _a28ac(d, k): return (d.get("acord28") or {}).get("additionalCoverages", {}).get(k)
def _a28mh(d, i, k):
    mhs = (d.get("acord28") or {}).get("mortgageholders", [])
    return mhs[i].get(k, "") if i < len(mhs) else ""


# ═════════════════════════════════════════════════════════════════════
# ACORD 25 — Verified field names from PDF dump
# ═════════════════════════════════════════════════════════════════════
ACORD25_FIELDS = {
    # ── Header ──────────────────────────────────────────────────────
    "Form_CompletionDate_A":                         lambda d: datetime.now().strftime("%m/%d/%Y"),
    "CertificateOfInsurance_CertificateNumberIdentifier_A": lambda d: _a25(d, "certificateNumber"),
    "CertificateOfInsurance_RevisionNumberIdentifier_A":    lambda d: "",

    # ── Producer ────────────────────────────────────────────────────
    "Producer_FullName_A":                           lambda d: d.get("producer", {}).get("name", ""),
    "Producer_MailingAddress_LineOne_A":              lambda d: _al1(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_LineTwo_A":              lambda d: _al2(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_CityName_A":             lambda d: _aci(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_StateOrProvinceCode_A":  lambda d: _ast(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_PostalCode_A":           lambda d: _azp(d.get("producer", {}).get("address", "")),
    "Producer_ContactPerson_FullName_A":              lambda d: d.get("producer", {}).get("contactName", ""),
    "Producer_ContactPerson_PhoneNumber_A":           lambda d: d.get("producer", {}).get("phone", ""),
    "Producer_FaxNumber_A":                           lambda d: d.get("producer", {}).get("fax", ""),
    "Producer_ContactPerson_EmailAddress_A":          lambda d: d.get("producer", {}).get("email", ""),
    "Producer_AuthorizedRepresentative_Signature_A":  lambda d: "",

    # ── Insured ─────────────────────────────────────────────────────
    "NamedInsured_FullName_A":                       lambda d: d.get("insured", {}).get("name", ""),
    "NamedInsured_MailingAddress_LineOne_A":          lambda d: _al1(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_LineTwo_A":          lambda d: _al2(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_CityName_A":         lambda d: _aci(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_StateOrProvinceCode_A": lambda d: _ast(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_PostalCode_A":       lambda d: _azp(d.get("insured", {}).get("address", "")),

    # ── Carriers A–F ────────────────────────────────────────────────
    "Insurer_FullName_A": lambda d: _carrier(d, "A"),
    "Insurer_NAICCode_A": lambda d: _carrier_naic(d, "A"),
    "Insurer_FullName_B": lambda d: _carrier(d, "B"),
    "Insurer_NAICCode_B": lambda d: _carrier_naic(d, "B"),
    "Insurer_FullName_C": lambda d: _carrier(d, "C"),
    "Insurer_NAICCode_C": lambda d: _carrier_naic(d, "C"),
    "Insurer_FullName_D": lambda d: _carrier(d, "D"),
    "Insurer_NAICCode_D": lambda d: _carrier_naic(d, "D"),
    "Insurer_FullName_E": lambda d: _carrier(d, "E"),
    "Insurer_NAICCode_E": lambda d: _carrier_naic(d, "E"),
    "Insurer_FullName_F": lambda d: _carrier(d, "F"),
    "Insurer_NAICCode_F": lambda d: _carrier_naic(d, "F"),

    # ── General Liability ───────────────────────────────────────────
    "GeneralLiability_InsurerLetterCode_A":          lambda d: _gl(d, "insurerLetter") or ("A" if _has_gl(d) else ""),
    "Policy_GeneralLiability_PolicyNumberIdentifier_A": lambda d: _gl(d, "policyNumber"),
    "Policy_GeneralLiability_EffectiveDate_A":       lambda d: _gl(d, "effectiveDate"),
    "Policy_GeneralLiability_ExpirationDate_A":      lambda d: _gl(d, "expirationDate"),

    "GeneralLiability_CoverageIndicator_A":          lambda d: _has_gl(d),
    "GeneralLiability_ClaimsMadeIndicator_A":        lambda d: bool(_gl(d, "claimsMade")),
    "GeneralLiability_OccurrenceIndicator_A":        lambda d: bool(_gl(d, "occurrence")),

    "GeneralLiability_EachOccurrence_LimitAmount_A":                         lambda d: _glL(d, "eachOccurrence"),
    "GeneralLiability_FireDamageRentedPremises_EachOccurrenceLimitAmount_A":  lambda d: _glL(d, "damageToRentedPremises"),
    "GeneralLiability_MedicalExpense_EachPersonLimitAmount_A":               lambda d: _glL(d, "medicalExpense"),
    "GeneralLiability_PersonalAndAdvertisingInjury_LimitAmount_A":           lambda d: _glL(d, "personalAdvInjury"),
    "GeneralLiability_GeneralAggregate_LimitAmount_A":                       lambda d: _glL(d, "generalAggregate"),
    "GeneralLiability_ProductsAndCompletedOperations_AggregateLimitAmount_A": lambda d: _glL(d, "productsCompOp"),

    "GeneralLiability_GeneralAggregate_LimitAppliesPerPolicyIndicator_A":   lambda d: _has_gl(d),
    "GeneralLiability_GeneralAggregate_LimitAppliesPerProjectIndicator_A":  lambda d: False,
    "GeneralLiability_GeneralAggregate_LimitAppliesPerLocationIndicator_A": lambda d: False,
    "GeneralLiability_GeneralAggregate_LimitAppliesToOtherIndicator_A":     lambda d: False,
    "GeneralLiability_GeneralAggregate_LimitAppliesToCode_A":               lambda d: "",

    "GeneralLiability_OtherCoverageIndicator_A":     lambda d: False,
    "GeneralLiability_OtherCoverageIndicator_B":     lambda d: False,
    "GeneralLiability_OtherCoverageDescription_A":   lambda d: "",
    "GeneralLiability_OtherCoverageDescription_B":   lambda d: "",
    "GeneralLiability_OtherCoverageLimitAmount_A":   lambda d: "",
    "GeneralLiability_OtherCoverageLimitDescription_A": lambda d: "",

    # GL endorsements
    "CertificateOfInsurance_GeneralLiability_AdditionalInsuredCode_A": lambda d: "Y" if _gl(d, "policyNumber") and _a25e(d, "additionalInsured") else "",
    "Policy_GeneralLiability_SubrogationWaivedCode_A":                 lambda d: "Y" if _gl(d, "policyNumber") and _a25e(d, "waiverOfSubrogation") else "",

    # ── Automobile Liability ────────────────────────────────────────
    "Vehicle_InsurerLetterCode_A":                       lambda d: _au(d, "insurerLetter") or ("A" if _has_auto(d) else ""),
    "Policy_AutomobileLiability_PolicyNumberIdentifier_A": lambda d: _au(d, "policyNumber"),
    "Policy_AutomobileLiability_EffectiveDate_A":        lambda d: _au(d, "effectiveDate"),
    "Policy_AutomobileLiability_ExpirationDate_A":       lambda d: _au(d, "expirationDate"),

    "Vehicle_AnyAutoIndicator_A":         lambda d: str(_au(d, "autoType")).lower() in ("any auto", "any"),
    "Vehicle_AllOwnedAutosIndicator_A":   lambda d: "owned" in str(_au(d, "autoType")).lower(),
    "Vehicle_ScheduledAutosIndicator_A":  lambda d: "scheduled" in str(_au(d, "autoType")).lower(),
    "Vehicle_HiredAutosIndicator_A":      lambda d: "hired" in str(_au(d, "autoType")).lower(),
    "Vehicle_NonOwnedAutosIndicator_A":   lambda d: "non-owned" in str(_au(d, "autoType")).lower() or "non owned" in str(_au(d, "autoType")).lower(),
    "Vehicle_OtherCoveredAutoIndicator_A": lambda d: False,
    "Vehicle_OtherCoveredAutoIndicator_B": lambda d: False,
    "Vehicle_OtherCoveredAutoDescription_A": lambda d: "",
    "Vehicle_OtherCoveredAutoDescription_B": lambda d: "",

    "Vehicle_CombinedSingleLimit_EachAccidentAmount_A": lambda d: _fm(_au(d, "combinedSingleLimit")),
    "Vehicle_BodilyInjury_PerPersonLimitAmount_A":      lambda d: "",
    "Vehicle_BodilyInjury_PerAccidentLimitAmount_A":    lambda d: "",
    "Vehicle_PropertyDamage_PerAccidentLimitAmount_A":  lambda d: "",
    "Vehicle_OtherCoverage_CoverageDescription_A":      lambda d: "",
    "Vehicle_OtherCoverage_LimitAmount_A":              lambda d: "",

    "CertificateOfInsurance_AutomobileLiability_AdditionalInsuredCode_A": lambda d: "Y" if _au(d, "policyNumber") and _a25e(d, "additionalInsured") else "",
    "Policy_AutomobileLiability_SubrogationWaivedCode_A":                 lambda d: "Y" if _au(d, "policyNumber") and _a25e(d, "waiverOfSubrogation") else "",

    # ── Umbrella / Excess ───────────────────────────────────────────
    "ExcessUmbrella_InsurerLetterCode_A":            lambda d: _um(d, "insurerLetter") or ("A" if _has_umbrella(d) else ""),
    "Policy_ExcessLiability_PolicyNumberIdentifier_A": lambda d: _um(d, "policyNumber"),
    "Policy_ExcessLiability_EffectiveDate_A":        lambda d: _um(d, "effectiveDate"),
    "Policy_ExcessLiability_ExpirationDate_A":       lambda d: _um(d, "expirationDate"),

    "Policy_PolicyType_UmbrellaIndicator_A": lambda d: str(_um(d, "type")).lower() in ("umbrella", ""),
    "Policy_PolicyType_ExcessIndicator_A":   lambda d: str(_um(d, "type")).lower() == "excess",
    "ExcessUmbrella_OccurrenceIndicator_A":  lambda d: bool(_um(d, "eachOccurrence")),
    "ExcessUmbrella_ClaimsMadeIndicator_A":  lambda d: False,
    "ExcessUmbrella_DeductibleIndicator_A":  lambda d: False,
    "ExcessUmbrella_RetentionIndicator_A":   lambda d: bool(_um(d, "retention")),

    "ExcessUmbrella_Umbrella_EachOccurrenceAmount_A":        lambda d: _fm(_um(d, "eachOccurrence")),
    "ExcessUmbrella_Umbrella_AggregateAmount_A":             lambda d: _fm(_um(d, "aggregate")),
    "ExcessUmbrella_Umbrella_DeductibleOrRetentionAmount_A": lambda d: _fm(_um(d, "retention")),
    "ExcessUmbrella_OtherCoverageDescription_A":             lambda d: "",
    "ExcessUmbrella_OtherCoverageLimitAmount_A":             lambda d: "",

    "CertificateOfInsurance_ExcessLiability_AdditionalInsuredCode_A": lambda d: "Y" if _um(d, "policyNumber") and _a25e(d, "additionalInsured") else "",
    "Policy_ExcessLiability_SubrogationWaivedCode_A":                 lambda d: "Y" if _um(d, "policyNumber") and _a25e(d, "waiverOfSubrogation") else "",

    # ── Workers Compensation ────────────────────────────────────────
    "WorkersCompensationEmployersLiability_InsurerLetterCode_A":    lambda d: _wc(d, "insurerLetter") or ("A" if _has_wc(d) else ""),
    "Policy_WorkersCompensationAndEmployersLiability_PolicyNumberIdentifier_A": lambda d: _wc(d, "policyNumber"),
    "Policy_WorkersCompensationAndEmployersLiability_EffectiveDate_A":         lambda d: _wc(d, "effectiveDate"),
    "Policy_WorkersCompensationAndEmployersLiability_ExpirationDate_A":        lambda d: _wc(d, "expirationDate"),

    "WorkersCompensationEmployersLiability_WorkersCompensationStatutoryLimitIndicator_A": lambda d: bool(_wc(d, "eachAccident")),
    "WorkersCompensationEmployersLiability_OtherCoverageIndicator_A":    lambda d: False,
    "WorkersCompensationEmployersLiability_OtherCoverageDescription_A":  lambda d: "",
    "WorkersCompensationEmployersLiability_AnyPersonsExcludedIndicator_A": lambda d: "",

    "WorkersCompensationEmployersLiability_EmployersLiability_EachAccidentLimitAmount_A":       lambda d: _fm(_wc(d, "eachAccident")),
    "WorkersCompensationEmployersLiability_EmployersLiability_DiseaseEachEmployeeLimitAmount_A": lambda d: _fm(_wc(d, "diseaseEachEmployee")),
    "WorkersCompensationEmployersLiability_EmployersLiability_DiseasePolicyLimitAmount_A":      lambda d: _fm(_wc(d, "diseasePolicyLimit")),

    "Policy_WorkersCompensation_SubrogationWaivedCode_A": lambda d: "Y" if _wc(d, "policyNumber") and _a25e(d, "waiverOfSubrogation") else "",

    # ── Other Policy (blank) ────────────────────────────────────────
    "OtherPolicy_InsurerLetterCode_A":       lambda d: "",
    "OtherPolicy_OtherPolicyDescription_A":  lambda d: "",
    "OtherPolicy_PolicyNumberIdentifier_A":  lambda d: "",
    "OtherPolicy_PolicyEffectiveDate_A":     lambda d: "",
    "OtherPolicy_PolicyExpirationDate_A":    lambda d: "",
    "OtherPolicy_CoverageCode_A":            lambda d: "",
    "OtherPolicy_CoverageCode_B":            lambda d: "",
    "OtherPolicy_CoverageCode_C":            lambda d: "",
    "OtherPolicy_CoverageLimitAmount_A":     lambda d: "",
    "OtherPolicy_CoverageLimitAmount_B":     lambda d: "",
    "OtherPolicy_CoverageLimitAmount_C":     lambda d: "",
    "CertificateOfInsurance_OtherPolicy_AdditionalInsuredCode_A": lambda d: "",
    "OtherPolicy_SubrogationWaivedCode_A":   lambda d: "",

    # ── Description of Operations ───────────────────────────────────
    "CertificateOfLiabilityInsurance_ACORDForm_RemarkText_A": lambda d: _a25(d, "descriptionOfOperations"),

    # ── Certificate Holder ──────────────────────────────────────────
    "CertificateHolder_FullName_A":                      lambda d: (d.get("acord25") or {}).get("certificateHolder", {}).get("name", ""),
    "CertificateHolder_MailingAddress_LineOne_A":         lambda d: _al1((d.get("acord25") or {}).get("certificateHolder", {}).get("address", "")),
    "CertificateHolder_MailingAddress_LineTwo_A":         lambda d: _al2((d.get("acord25") or {}).get("certificateHolder", {}).get("address", "")),
    "CertificateHolder_MailingAddress_CityName_A":        lambda d: _aci((d.get("acord25") or {}).get("certificateHolder", {}).get("address", "")),
    "CertificateHolder_MailingAddress_StateOrProvinceCode_A": lambda d: _ast((d.get("acord25") or {}).get("certificateHolder", {}).get("address", "")),
    "CertificateHolder_MailingAddress_PostalCode_A":      lambda d: _azp((d.get("acord25") or {}).get("certificateHolder", {}).get("address", "")),
}


# ═════════════════════════════════════════════════════════════════════
# ACORD 27 — Verified field names from PDF dump
# ═════════════════════════════════════════════════════════════════════
ACORD27_FIELDS = {
    # ── Header ──────────────────────────────────────────────────────
    "Form_CompletionDate_A": lambda d: datetime.now().strftime("%m/%d/%Y"),

    # ── Producer ────────────────────────────────────────────────────
    "Producer_FullName_A":                          lambda d: d.get("producer", {}).get("name", ""),
    "Producer_MailingAddress_LineOne_A":             lambda d: _al1(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_LineTwo_A":             lambda d: _al2(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_CityName_A":            lambda d: _aci(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_StateOrProvinceCode_A": lambda d: _ast(d.get("producer", {}).get("address", "")),
    "Producer_MailingAddress_PostalCode_A":          lambda d: _azp(d.get("producer", {}).get("address", "")),
    "Producer_ContactPerson_PhoneNumber_A":          lambda d: d.get("producer", {}).get("phone", ""),
    "Producer_FaxNumber_A":                          lambda d: d.get("producer", {}).get("fax", ""),
    "Producer_ContactPerson_EmailAddress_A":         lambda d: d.get("producer", {}).get("email", ""),
    "Producer_CustomerIdentifier_A":                 lambda d: "",
    "Producer_AuthorizedRepresentative_Signature_A": lambda d: "",

    # ── Insurer (carrier) ───────────────────────────────────────────
    "Insurer_FullName_A":                           lambda d: _carrier(d, (d.get("acord27") or {}).get("insurerLetter", "A")),
    "Insurer_MailingAddress_AddressLineOne_A":      lambda d: "",
    "Insurer_MailingAddress_AddressLineTwo_A":      lambda d: "",
    "Insurer_MailingAddress_CityName_A":            lambda d: "",
    "Insurer_MailingAddress_StateOrProvinceCode_A": lambda d: "",
    "Insurer_MailingAddress_PostalCode_A":          lambda d: "",
    "Insurer_ProducerIdentifier_A":                 lambda d: "",
    "Insurer_SubProducerIdentifier_A":              lambda d: "",

    # ── Named Insured ───────────────────────────────────────────────
    "NamedInsured_FullName_A":                          lambda d: d.get("insured", {}).get("name", ""),
    "NamedInsured_MailingAddress_LineOne_A":             lambda d: _al1(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_LineTwo_A":             lambda d: _al2(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_CityName_A":            lambda d: _aci(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_StateOrProvinceCode_A": lambda d: _ast(d.get("insured", {}).get("address", "")),
    "NamedInsured_MailingAddress_PostalCode_A":          lambda d: _azp(d.get("insured", {}).get("address", "")),

    # ── Policy ──────────────────────────────────────────────────────
    "Policy_PolicyNumberIdentifier_A": lambda d: _a27(d, "policyNumber"),
    "Policy_EffectiveDate_A":          lambda d: _a27(d, "effectiveDate"),
    "Policy_ExpirationDate_A":         lambda d: _a27(d, "expirationDate"),

    # ── Perils / Cause of Loss ──────────────────────────────────────
    "Policy_PolicyType_BasicIndicator_A":   lambda d: str(_a27(d, "causeOfLoss")).lower() == "basic",
    "Policy_PolicyType_BroadIndicator_A":   lambda d: str(_a27(d, "causeOfLoss")).lower() == "broad",
    "Policy_PolicyType_SpecialIndicator_A": lambda d: "special" in str(_a27(d, "causeOfLoss")).lower(),
    "Policy_PolicyType_OtherIndicator_A":   lambda d: False,
    "Policy_PolicyType_OtherDescription_A": lambda d: "",

    # ── Property Location ───────────────────────────────────────────
    "EvidenceOfProperty_PropertyDescription_A":                    lambda d: _a27(d, "propertyAddress"),
    "EvidenceOfProperty_PhysicalAddress_StreetLineOne_A":          lambda d: _al1(_a27(d, "propertyAddress")),
    "EvidenceOfProperty_PhysicalAddress_StreetLineTwo_A":          lambda d: _al2(_a27(d, "propertyAddress")),
    "EvidenceOfProperty_PhysicalAddress_CityName_A":               lambda d: _aci(_a27(d, "propertyAddress")),
    "EvidenceOfProperty_PhysicalAddress_StateOrProvinceCode_A":    lambda d: _ast(_a27(d, "propertyAddress")),
    "EvidenceOfProperty_PhysicalAddress_PostalCode_A":             lambda d: _azp(_a27(d, "propertyAddress")),
    "EvidenceOfProperty_PhysicalAddress_CountyName_A":             lambda d: "",
    "EvidenceOfProperty_PriorEvidenceDate_A":                      lambda d: "",
    "EvidenceOfProperty_ContinuousBasisIndicator_A":               lambda d: False,

    # ── Coverage Rows A–J ───────────────────────────────────────────
    "EvidenceOfProperty_CoverageDescription_A": lambda d: "Building" if _a27c(d, "building") else "",
    "EvidenceOfProperty_LimitAmount_A":         lambda d: _fm(_a27c(d, "building")),
    "EvidenceOfProperty_DeductibleAmount_A":    lambda d: _fm(_a27(d, "deductible")),

    "EvidenceOfProperty_CoverageDescription_B": lambda d: "Business Personal Property" if _a27c(d, "personalProperty") else "",
    "EvidenceOfProperty_LimitAmount_B":         lambda d: _fm(_a27c(d, "personalProperty")),
    "EvidenceOfProperty_DeductibleAmount_B":    lambda d: _fm(_a27(d, "deductible")) if _a27c(d, "personalProperty") else "",

    "EvidenceOfProperty_CoverageDescription_C": lambda d: "Business Income" if _a27c(d, "businessIncome") else "",
    "EvidenceOfProperty_LimitAmount_C":         lambda d: _fm(_a27c(d, "businessIncome")),
    "EvidenceOfProperty_DeductibleAmount_C":    lambda d: "",

    "EvidenceOfProperty_CoverageDescription_D": lambda d: "Flood" if _a27c(d, "flood") else "",
    "EvidenceOfProperty_LimitAmount_D":         lambda d: _fm(_a27c(d, "flood")),
    "EvidenceOfProperty_DeductibleAmount_D":    lambda d: "",

    "EvidenceOfProperty_CoverageDescription_E": lambda d: "Earthquake" if _a27c(d, "earthquake") else "",
    "EvidenceOfProperty_LimitAmount_E":         lambda d: _fm(_a27c(d, "earthquake")),
    "EvidenceOfProperty_DeductibleAmount_E":    lambda d: "",

    # Rows F–J (empty by default)
    **{f"EvidenceOfProperty_CoverageDescription_{x}": (lambda d: "") for x in "FGHIJ"},
    **{f"EvidenceOfProperty_LimitAmount_{x}":         (lambda d: "") for x in "FGHIJ"},
    **{f"EvidenceOfProperty_DeductibleAmount_{x}":    (lambda d: "") for x in "FGHIJ"},

    # ── Remarks ─────────────────────────────────────────────────────
    "EvidenceOfProperty_RemarkText_A": lambda d: "",

    # ── Additional Interest / Mortgageholder ────────────────────────
    "AdditionalInterest_FullName_A":                          lambda d: _a27m(d, "name"),
    "AdditionalInterest_MailingAddress_LineOne_A":             lambda d: _al1(_a27m(d, "address")),
    "AdditionalInterest_MailingAddress_LineTwo_A":             lambda d: _al2(_a27m(d, "address")),
    "AdditionalInterest_MailingAddress_CityName_A":            lambda d: _aci(_a27m(d, "address")),
    "AdditionalInterest_MailingAddress_StateOrProvinceCode_A": lambda d: _ast(_a27m(d, "address")),
    "AdditionalInterest_MailingAddress_PostalCode_A":          lambda d: _azp(_a27m(d, "address")),
    "AdditionalInterest_AccountNumberIdentifier_A":            lambda d: _a27m(d, "loanNumber"),
    "AdditionalInterest_AccountNumberIdentifier_B":            lambda d: "",

    "AdditionalInterest_Interest_MortgageeIndicator_A":         lambda d: bool(_a27m(d, "name")),
    "AdditionalInterest_Interest_AdditionalInsuredIndicator_A": lambda d: False,
    "AdditionalInterest_Interest_LendersLossPayableIndicator_A": lambda d: False,
    "AdditionalInterest_Interest_LossPayeeIndicator_A":         lambda d: False,
    "AdditionalInterest_Interest_OtherIndicator_A":             lambda d: False,
    "AdditionalInterest_Interest_OtherDescription_A":           lambda d: "",
}


# ═════════════════════════════════════════════════════════════════════
# ACORD 30 — Verified field names from PDF dump (XFA-style F[0].P1[0]. prefix)
# Note: The extraction JSON uses "acord28" key for this form's data
# since ACORD 28/30 both cover property/garage. Adjust if needed.
# ═════════════════════════════════════════════════════════════════════

# ACORD 30 accessors — reading from acord28 in extracted JSON
# If your extraction outputs a separate "acord30" key, change these.
def _a30(d, k): return (d.get("acord30") or d.get("acord28") or {}).get(k, "")
def _a30_gl(d, k): return (d.get("acord30") or d.get("acord28") or {}).get("garageLiability", {}).get(k)
def _a30_gk(d, k): return (d.get("acord30") or d.get("acord28") or {}).get("garageKeepers", {}).get(k)
def _a30_cgl(d, k): return (d.get("acord30") or d.get("acord28") or {}).get("commercialGL", {}).get(k)
def _a30_umb(d, k):
    v = (d.get("acord30") or d.get("acord28") or {}).get("umbrella", {}).get(k, "")
    return _fm(v) if isinstance(v, (int, float)) else v
def _a30_wc(d, k):
    v = (d.get("acord30") or d.get("acord28") or {}).get("workersComp", {}).get(k, "")
    return _fm(v) if isinstance(v, (int, float)) else v
def _a30e(d, k): return (d.get("acord30") or d.get("acord28") or {}).get("endorsements", {}).get(k, False)
def _a30_ch(d, k): return (d.get("acord30") or d.get("acord28") or {}).get("certificateHolder", {}).get(k, "")

def _has_a30_garage(d):
    return bool(_a30(d, "policyNumber") or _a30_gl(d, "autoOnlyEachAccident"))

def _has_a30_cgl(d):
    return bool(_a30_cgl(d, "included") or _a30_cgl(d, "eachOccurrence") or _a30_cgl(d, "generalAggregate"))

def _has_a30_umbrella(d):
    return bool(_a30_umb(d, "policyNumber") or _a30_umb(d, "eachOccurrence"))

def _has_a30_wc(d):
    return bool(_a30_wc(d, "policyNumber") or _a30_wc(d, "eachAccident"))

ACORD30_FIELDS = {
    # ── Header ──────────────────────────────────────────────────────
    "F[0].P1[0].Form_CompletionDate_A[0]": lambda d: datetime.now().strftime("%m/%d/%Y"),
    "F[0].P1[0].CertificateOfInsurance_CertificateNumberIdentifier_A[0]": lambda d: "",
    "F[0].P1[0].CertificateOfInsurance_RevisionNumberIdentifier_A[0]":    lambda d: "",

    # ── Producer ────────────────────────────────────────────────────
    "F[0].P1[0].Producer_FullName_A[0]":                          lambda d: d.get("producer", {}).get("name", ""),
    "F[0].P1[0].Producer_MailingAddress_LineOne_A[0]":             lambda d: _al1(d.get("producer", {}).get("address", "")),
    "F[0].P1[0].Producer_MailingAddress_LineTwo_A[0]":             lambda d: _al2(d.get("producer", {}).get("address", "")),
    "F[0].P1[0].Producer_MailingAddress_CityName_A[0]":            lambda d: _aci(d.get("producer", {}).get("address", "")),
    "F[0].P1[0].Producer_MailingAddress_StateOrProvinceCode_A[0]": lambda d: _ast(d.get("producer", {}).get("address", "")),
    "F[0].P1[0].Producer_MailingAddress_PostalCode_A[0]":          lambda d: _azp(d.get("producer", {}).get("address", "")),
    "F[0].P1[0].Producer_ContactPerson_FullName_A[0]":             lambda d: d.get("producer", {}).get("contactName", ""),
    "F[0].P1[0].Producer_ContactPerson_PhoneNumber_A[0]":          lambda d: d.get("producer", {}).get("phone", ""),
    "F[0].P1[0].Producer_FaxNumber_A[0]":                          lambda d: d.get("producer", {}).get("fax", ""),
    "F[0].P1[0].Producer_ContactPerson_EmailAddress_A[0]":         lambda d: d.get("producer", {}).get("email", ""),
    "F[0].P1[0].Producer_CustomerIdentifier_A[0]":                 lambda d: "",
    "F[0].P1[0].Producer_AuthorizedRepresentative_Signature_A[0]": lambda d: "",

    # ── Insured ─────────────────────────────────────────────────────
    "F[0].P1[0].NamedInsured_FullName_A[0]":                          lambda d: d.get("insured", {}).get("name", ""),
    "F[0].P1[0].NamedInsured_MailingAddress_LineOne_A[0]":             lambda d: _al1(d.get("insured", {}).get("address", "")),
    "F[0].P1[0].NamedInsured_MailingAddress_LineTwo_A[0]":             lambda d: _al2(d.get("insured", {}).get("address", "")),
    "F[0].P1[0].NamedInsured_MailingAddress_CityName_A[0]":            lambda d: _aci(d.get("insured", {}).get("address", "")),
    "F[0].P1[0].NamedInsured_MailingAddress_StateOrProvinceCode_A[0]": lambda d: _ast(d.get("insured", {}).get("address", "")),
    "F[0].P1[0].NamedInsured_MailingAddress_PostalCode_A[0]":          lambda d: _azp(d.get("insured", {}).get("address", "")),

    # ── Carriers A–F ────────────────────────────────────────────────
    "F[0].P1[0].Insurer_FullName_A[0]": lambda d: _carrier(d, "A"),
    "F[0].P1[0].Insurer_NAICCode_A[0]": lambda d: _carrier_naic(d, "A"),
    "F[0].P1[0].Insurer_FullName_B[0]": lambda d: _carrier(d, "B"),
    "F[0].P1[0].Insurer_NAICCode_B[0]": lambda d: _carrier_naic(d, "B"),
    "F[0].P1[0].Insurer_FullName_C[0]": lambda d: _carrier(d, "C"),
    "F[0].P1[0].Insurer_NAICCode_C[0]": lambda d: _carrier_naic(d, "C"),
    "F[0].P1[0].Insurer_FullName_D[0]": lambda d: _carrier(d, "D"),
    "F[0].P1[0].Insurer_NAICCode_D[0]": lambda d: _carrier_naic(d, "D"),
    "F[0].P1[0].Insurer_FullName_E[0]": lambda d: _carrier(d, "E"),
    "F[0].P1[0].Insurer_NAICCode_E[0]": lambda d: _carrier_naic(d, "E"),
    "F[0].P1[0].Insurer_FullName_F[0]": lambda d: _carrier(d, "F"),
    "F[0].P1[0].Insurer_NAICCode_F[0]": lambda d: _carrier_naic(d, "F"),

    # ── Garage Liability (Row A) ────────────────────────────────────
    "F[0].P1[0].GarageLiability_InsurerLetterCode_A[0]":  lambda d: _a30(d, "insurerLetter") or ("A" if _has_a30_garage(d) else ""),
    "F[0].P1[0].Policy_PolicyNumberIdentifier_A[0]":      lambda d: _a30(d, "policyNumber"),
    "F[0].P1[0].Policy_EffectiveDate_A[0]":               lambda d: _a30(d, "effectiveDate"),
    "F[0].P1[0].Policy_ExpirationDate_A[0]":              lambda d: _a30(d, "expirationDate"),

    "F[0].P1[0].GarageLiability_AllOwnedAutosIndicator_A[0]":                       lambda d: bool(_a30_gl(d, "allOwnedAutos")),
    "F[0].P1[0].GarageLiability_HiredAutosIndicator_A[0]":                          lambda d: bool(_a30_gl(d, "hiredAutos")),
    "F[0].P1[0].GarageLiability_NonOwnedAutosUsedInGarageBusinessIndicator_A[0]":    lambda d: bool(_a30_gl(d, "nonOwnedAutos")),
    "F[0].P1[0].GarageLiability_OtherIndicator_A[0]":                               lambda d: False,
    "F[0].P1[0].GarageLiability_OtherDescription_A[0]":                             lambda d: "",

    "F[0].P1[0].GarageLiability_AutoOnly_EachAccidentLimitAmount_A[0]":             lambda d: _fm(_a30_gl(d, "autoOnlyEachAccident")),
    "F[0].P1[0].GarageLiability_OtherThanAutoOnly_EachAccidentLimitAmount_A[0]":    lambda d: _fm(_a30_gl(d, "otherThanAutoOnly")),
    "F[0].P1[0].GarageLiability_OtherThanAutoOnly_AggregateLimitAmount_A[0]":       lambda d: _fm(_a30_gl(d, "autoOnlyAggregate")),

    # Endorsements per row — Row A = Garage Liability
    "F[0].P1[0].CertificateOfInsurance_AdditionalInsuredCode_A[0]": lambda d: "Y" if _a30(d, "policyNumber") and _a30e(d, "additionalInsured") else "",
    "F[0].P1[0].Policy_SubrogationWaivedCode_A[0]":                 lambda d: "Y" if _a30(d, "policyNumber") and _a30e(d, "waiverOfSubrogation") else "",

    # ── Garage Keepers (Row B) ──────────────────────────────────────
    "F[0].P1[0].GarageKeepersLiability_InsurerLetterCode_A[0]":    lambda d: _a30(d, "insurerLetter") or ("A" if _has_a30_garage(d) else ""),
    "F[0].P1[0].Policy_PolicyNumberIdentifier_B[0]":               lambda d: _a30(d, "policyNumber"),
    "F[0].P1[0].Policy_EffectiveDate_B[0]":                        lambda d: _a30(d, "effectiveDate"),
    "F[0].P1[0].Policy_ExpirationDate_B[0]":                       lambda d: _a30(d, "expirationDate"),

    "F[0].P1[0].GarageKeepersLiability_LegalLiabilityIndicator_A[0]": lambda d: bool(_a30_gk(d, "legalLiability")),
    "F[0].P1[0].GarageKeepersLiability_DirectBasisIndicator_A[0]":    lambda d: bool(_a30_gk(d, "directBasis")),
    "F[0].P1[0].GarageKeepersLiability_PrimaryIndicator_A[0]":        lambda d: bool(_a30_gk(d, "primary")),
    "F[0].P1[0].GarageKeepersLiability_ExcessIndicator_A[0]":         lambda d: bool(_a30_gk(d, "excess")),
    "F[0].P1[0].GarageKeepersLiability_ComprehensiveIndicator_A[0]":  lambda d: bool(_a30_gk(d, "comprehensive")),
    "F[0].P1[0].GarageKeepersLiability_SpecifiedPerilsIndicator_A[0]": lambda d: bool(_a30_gk(d, "specifiedPerils")),
    "F[0].P1[0].GarageKeepersLiability_CollisionIndicator_A[0]":      lambda d: bool(_a30_gk(d, "collision")),

    "F[0].P1[0].GarageKeepersLiability_ComprehensiveOrSpecifiedPerilsLimitAmount_A[0]": lambda d: _fm(_a30_gk(d, "comprehensive")),
    "F[0].P1[0].GarageKeepersLiability_ComprehensiveOrSpecifiedPerilsLimitAmount_B[0]": lambda d: "",
    "F[0].P1[0].GarageKeepersLiability_CollisionLimitAmount_A[0]":                      lambda d: _fm(_a30_gk(d, "collision")),
    "F[0].P1[0].GarageKeepersLiability_CollisionLimitAmount_B[0]":                      lambda d: "",
    "F[0].P1[0].GarageKeepersLiability_LocationProducerIdentifier_A[0]":                lambda d: "",
    "F[0].P1[0].GarageKeepersLiability_LocationProducerIdentifier_B[0]":                lambda d: "",
    "F[0].P1[0].GarageKeepersLiability_LocationProducerIdentifier_C[0]":                lambda d: "",
    "F[0].P1[0].GarageKeepersLiability_LocationProducerIdentifier_D[0]":                lambda d: "",

    "F[0].P1[0].CertificateOfInsurance_AdditionalInsuredCode_B[0]": lambda d: "",
    "F[0].P1[0].Policy_SubrogationWaivedCode_B[0]":                 lambda d: "",

    # ── General Liability (Row C) ───────────────────────────────────
    "F[0].P1[0].GeneralLiability_InsurerLetterCode_A[0]":          lambda d: (_a30(d, "insurerLetter") or ("A" if _has_a30_cgl(d) else "")) if _has_a30_cgl(d) else "",
    "F[0].P1[0].Policy_PolicyNumberIdentifier_C[0]":               lambda d: _a30(d, "policyNumber") if _a30_cgl(d, "included") else "",
    "F[0].P1[0].Policy_EffectiveDate_C[0]":                        lambda d: _a30(d, "effectiveDate") if _a30_cgl(d, "included") else "",
    "F[0].P1[0].Policy_ExpirationDate_C[0]":                       lambda d: _a30(d, "expirationDate") if _a30_cgl(d, "included") else "",

    "F[0].P1[0].GeneralLiability_CoverageIndicator_A[0]":         lambda d: _has_a30_cgl(d),
    "F[0].P1[0].GeneralLiability_ClaimsMadeIndicator_A[0]":       lambda d: False,
    "F[0].P1[0].GeneralLiability_OccurrenceIndicator_A[0]":       lambda d: bool(_a30_cgl(d, "included")),

    "F[0].P1[0].GeneralLiability_EachOccurrence_LimitAmount_A[0]":                        lambda d: _fm(_a30_cgl(d, "eachOccurrence")),
    "F[0].P1[0].GeneralLiability_FireDamageRentedPremises_EachOccurrenceLimitAmount_A[0]": lambda d: _fm(_a30_cgl(d, "damageToRentedPremises")),
    "F[0].P1[0].GeneralLiability_MedicalExpense_EachPersonLimitAmount_A[0]":               lambda d: _fm(_a30_cgl(d, "medicalExpense")),
    "F[0].P1[0].GeneralLiability_PersonalAndAdvertisingInjury_LimitAmount_A[0]":           lambda d: _fm(_a30_cgl(d, "personalAdvInjury")),
    "F[0].P1[0].GeneralLiability_GeneralAggregate_LimitAmount_A[0]":                       lambda d: _fm(_a30_cgl(d, "generalAggregate")),
    "F[0].P1[0].GeneralLiability_ProductsAndCompletedOperations_AggregateLimitAmount_A[0]": lambda d: _fm(_a30_cgl(d, "productsCompOp")),

    "F[0].P1[0].GeneralLiability_GeneralAggregate_LimitAppliesPerPolicyIndicator_A[0]":   lambda d: _has_a30_cgl(d),
    "F[0].P1[0].GeneralLiability_GeneralAggregate_LimitAppliesPerProjectIndicator_A[0]":  lambda d: False,
    "F[0].P1[0].GeneralLiability_GeneralAggregate_LimitAppliesPerLocationIndicator_A[0]": lambda d: False,

    "F[0].P1[0].GeneralLiability_OtherCoverageIndicator_A[0]":    lambda d: False,
    "F[0].P1[0].GeneralLiability_OtherCoverageIndicator_B[0]":    lambda d: False,
    "F[0].P1[0].GeneralLiability_OtherCoverageDescription_A[0]":  lambda d: "",
    "F[0].P1[0].GeneralLiability_OtherCoverageDescription_B[0]":  lambda d: "",
    "F[0].P1[0].GeneralLiability_OtherCoverageLimitAmount_A[0]":  lambda d: "",
    "F[0].P1[0].GeneralLiability_OtherCoverageLimitDescription_A[0]": lambda d: "",

    "F[0].P1[0].CertificateOfInsurance_AdditionalInsuredCode_C[0]": lambda d: "Y" if _a30_cgl(d, "included") and _a30e(d, "additionalInsured") else "",
    "F[0].P1[0].Policy_SubrogationWaivedCode_C[0]":                 lambda d: "Y" if _a30_cgl(d, "included") and _a30e(d, "waiverOfSubrogation") else "",

    # ── Other Policy (Row D — blank) ────────────────────────────────
    "F[0].P1[0].OtherPolicy_InsurerLetterCode_A[0]":    lambda d: "",
    "F[0].P1[0].OtherPolicy_OtherPolicyDescription_A[0]": lambda d: "",
    "F[0].P1[0].Policy_PolicyNumberIdentifier_D[0]":    lambda d: "",
    "F[0].P1[0].Policy_EffectiveDate_D[0]":             lambda d: "",
    "F[0].P1[0].Policy_ExpirationDate_D[0]":            lambda d: "",
    "F[0].P1[0].OtherPolicy_CoverageLimitAmount_A[0]":  lambda d: "",
    "F[0].P1[0].CertificateOfInsurance_AdditionalInsuredCode_D[0]": lambda d: "",
    "F[0].P1[0].Policy_SubrogationWaivedCode_D[0]":     lambda d: "",

    # ── Umbrella / Excess (Row E) ───────────────────────────────────
    "F[0].P1[0].ExcessUmbrella_InsurerLetterCode_A[0]":           lambda d: _a30_umb(d, "insurerLetter") or ("A" if _has_a30_umbrella(d) else ""),
    "F[0].P1[0].Policy_PolicyNumberIdentifier_E[0]":              lambda d: _a30_umb(d, "policyNumber"),
    "F[0].P1[0].Policy_EffectiveDate_E[0]":                       lambda d: _a30_umb(d, "effectiveDate"),
    "F[0].P1[0].Policy_ExpirationDate_E[0]":                      lambda d: _a30_umb(d, "expirationDate"),

    "F[0].P1[0].Policy_PolicyType_UmbrellaIndicator_A[0]": lambda d: bool(_a30_umb(d, "eachOccurrence")),
    "F[0].P1[0].Policy_PolicyType_ExcessIndicator_A[0]":   lambda d: False,
    "F[0].P1[0].ExcessUmbrella_OccurrenceIndicator_A[0]":  lambda d: bool(_a30_umb(d, "eachOccurrence")),
    "F[0].P1[0].ExcessUmbrella_ClaimsMadeIndicator_A[0]":  lambda d: False,
    "F[0].P1[0].ExcessUmbrella_DeductibleIndicator_A[0]":  lambda d: False,
    "F[0].P1[0].ExcessUmbrella_RetentionIndicator_A[0]":   lambda d: bool(_a30_umb(d, "retention")),

    "F[0].P1[0].ExcessUmbrella_Umbrella_EachOccurrenceAmount_A[0]":        lambda d: _fm(_a30_umb(d, "eachOccurrence")),
    "F[0].P1[0].ExcessUmbrella_Umbrella_AggregateAmount_A[0]":             lambda d: _fm(_a30_umb(d, "aggregate")),
    "F[0].P1[0].ExcessUmbrella_Umbrella_DeductibleOrRetentionAmount_A[0]": lambda d: _fm(_a30_umb(d, "retention")),
    "F[0].P1[0].ExcessUmbrella_OtherCoverageDescription_A[0]":             lambda d: "",
    "F[0].P1[0].ExcessUmbrella_OtherCoverageLimitAmount_A[0]":             lambda d: "",

    "F[0].P1[0].CertificateOfInsurance_AdditionalInsuredCode_F[0]": lambda d: "",
    "F[0].P1[0].Policy_SubrogationWaivedCode_E[0]":                 lambda d: "",

    # ── Workers Comp (Row F) ────────────────────────────────────────
    "F[0].P1[0].WorkersCompensationEmployersLiability_InsurerLetterCode_A[0]":    lambda d: _a30_wc(d, "insurerLetter") or ("A" if _has_a30_wc(d) else ""),
    "F[0].P1[0].Policy_PolicyNumberIdentifier_F[0]":                              lambda d: _a30_wc(d, "policyNumber"),
    "F[0].P1[0].Policy_EffectiveDate_F[0]":                                       lambda d: _a30_wc(d, "effectiveDate"),
    "F[0].P1[0].Policy_ExpirationDate_F[0]":                                      lambda d: _a30_wc(d, "expirationDate"),

    "F[0].P1[0].WorkersCompensationEmployersLiability_WorkersCompensationStatutoryLimitIndicator_A[0]": lambda d: bool(_a30_wc(d, "eachAccident")),
    "F[0].P1[0].WorkersCompensationEmployersLiability_OtherCoverageIndicator_A[0]":    lambda d: False,
    "F[0].P1[0].WorkersCompensationEmployersLiability_OtherCoverageDescription_A[0]":  lambda d: "",
    "F[0].P1[0].WorkersCompensationEmployersLiability_AnyPersonsExcludedIndicator_A[0]": lambda d: "",

    "F[0].P1[0].WorkersCompensationEmployersLiability_EmployersLiability_EachAccidentLimitAmount_A[0]":       lambda d: _fm(_a30_wc(d, "eachAccident")),
    "F[0].P1[0].WorkersCompensationEmployersLiability_EmployersLiability_DiseaseEachEmployeeLimitAmount_A[0]": lambda d: _fm(_a30_wc(d, "diseaseEachEmployee")),
    "F[0].P1[0].WorkersCompensationEmployersLiability_EmployersLiability_DiseasePolicyLimitAmount_A[0]":      lambda d: _fm(_a30_wc(d, "diseasePolicyLimit")),

    "F[0].P1[0].Policy_SubrogationWaivedCode_F[0]": lambda d: "Y" if _a30_wc(d, "policyNumber") and _a30e(d, "waiverOfSubrogation") else "",

    # ── Remarks ─────────────────────────────────────────────────────
    "F[0].P1[0].CertificateOfLiabilityInsurance_ACORDForm_RemarkText_A[0]": lambda d: _a30(d, "remarks"),

    # ── Certificate Holder ──────────────────────────────────────────
    "F[0].P1[0].CertificateHolder_FullName_A[0]":                          lambda d: _a30_ch(d, "name"),
    "F[0].P1[0].CertificateHolder_MailingAddress_LineOne_A[0]":             lambda d: _al1(_a30_ch(d, "address")),
    "F[0].P1[0].CertificateHolder_MailingAddress_LineTwo_A[0]":             lambda d: _al2(_a30_ch(d, "address")),
    "F[0].P1[0].CertificateHolder_MailingAddress_CityName_A[0]":            lambda d: _aci(_a30_ch(d, "address")),
    "F[0].P1[0].CertificateHolder_MailingAddress_StateOrProvinceCode_A[0]": lambda d: _ast(_a30_ch(d, "address")),
    "F[0].P1[0].CertificateHolder_MailingAddress_PostalCode_A[0]":          lambda d: _azp(_a30_ch(d, "address")),
}

# ═════════════════════════════════════════════════════════════════════
# ACORD 28 — TODO: run dump_pdf_fields.py on acord28.pdf and populate
# ═════════════════════════════════════════════════════════════════════
ACORD28_FIELDS = {}  # Placeholder — populate after field dump


# ═════════════════════════════════════════════════════════════════════
# PDF Filling Engine
# ═════════════════════════════════════════════════════════════════════

def _discover_checkbox_on_state(annot) -> str:
    """Find the 'on' state name from a checkbox's appearance dict."""
    ap = annot.get("/AP")
    if not ap:
        return "/Yes"
    try:
        normal = ap.get_object().get("/N") if hasattr(ap, "get_object") else ap.get("/N")
        if normal:
            n_obj = normal.get_object() if hasattr(normal, "get_object") else normal
            if hasattr(n_obj, "keys"):
                for key in n_obj.keys():
                    if str(key) != "/Off":
                        return str(key)
    except Exception:
        pass
    return "/Yes"


def _get_field_type(annot) -> str:
    """Get field type, walking up parent chain if needed."""
    ft = annot.get("/FT", "")
    if ft:
        return str(ft)
    parent = annot.get("/Parent")
    while parent:
        po = parent.get_object() if hasattr(parent, "get_object") else parent
        ft = po.get("/FT", "")
        if ft:
            return str(ft)
        parent = po.get("/Parent")
    return ""


def _get_qualified_name(annot) -> str:
    """Build fully qualified field name by walking parent chain."""
    t = annot.get("/T", "")
    parts = [str(t)] if t else []
    parent = annot.get("/Parent")
    while parent:
        po = parent.get_object() if hasattr(parent, "get_object") else parent
        pt = po.get("/T", "")
        if pt:
            parts.insert(0, str(pt))
        parent = po.get("/Parent")
    return ".".join(parts) if parts else ""


def fill_pdf(template_path: str, output_path: str, field_map: dict, data: dict):
    """Fill a PDF form. field_map: { "PDF Field Name": lambda data -> value }"""
    reader = PdfReader(template_path)
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

    # Resolve all values
    fill_values = {}
    for field_name, resolver in field_map.items():
        try:
            val = resolver(data) if callable(resolver) else resolver
        except Exception as e:
            print(f"  ⚠ Error resolving {field_name}: {e}")
            continue
        # Skip empty/false/None — they don't need to be written
        if val is None or val == "" or val is False:
            continue
        fill_values[field_name] = val

    if not fill_values:
        print(f"  ⚠ No values to fill")
        return output_path

    # Fill by walking annotations on each page
    filled = 0
    matched_fields = set()

    for page in writer.pages:
        if "/Annots" not in page:
            continue
        for annot_ref in page["/Annots"]:
            annot = annot_ref.get_object()
            short_name = str(annot.get("/T", ""))
            qualified = _get_qualified_name(annot)

            # Match: try short name first, then qualified
            val = fill_values.get(short_name) or fill_values.get(qualified)
            if val is None:
                continue

            ft = _get_field_type(annot)

            if ft == "/Btn":
                # Checkbox or radio button
                if val is True or (isinstance(val, str) and val.upper() in ("Y", "YES")):
                    on_state = _discover_checkbox_on_state(annot)
                    annot.update({
                        NameObject("/V"): NameObject(on_state),
                        NameObject("/AS"): NameObject(on_state),
                    })
                    filled += 1
                    matched_fields.add(short_name or qualified)
            else:
                # Text field
                annot.update({
                    NameObject("/V"): TextStringObject(str(val)),
                })
                if "/AP" in annot:
                    del annot["/AP"]
                filled += 1
                matched_fields.add(short_name or qualified)

    # Force PDF viewers to re-render field appearances
    if "/AcroForm" in writer._root_object:
        writer._root_object["/AcroForm"].update({
            NameObject("/NeedAppearances"): BooleanObject(True)
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        writer.write(f)

    # Report unmatched
    unmatched = set(fill_values.keys()) - matched_fields
    print(f"  ✓ Filled {filled} fields → {output_path}")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} mapped fields not found in PDF:")
        for s in sorted(unmatched)[:10]:
            print(f"      - {s}")
        if len(unmatched) > 10:
            print(f"      ... and {len(unmatched) - 10} more")

    return output_path


# ═════════════════════════════════════════════════════════════════════
# Pipeline
# ═════════════════════════════════════════════════════════════════════

def determine_forms(data: dict) -> list[str]:
    forms = []
    if data.get("acord25") is not None:
        forms.append("25")
    if data.get("acord27") is not None:
        forms.append("27")
    if data.get("acord28") is not None:
        forms.append("28")
    if data.get("acord30") is not None:
        forms.append("30")
    return forms


def fill_acord_forms(data: dict, output_dir: str, forms: list[str] = None):
    os.makedirs(output_dir, exist_ok=True)
    if forms is None:
        forms = determine_forms(data)

    insured = data.get("insured", {}).get("name", "unknown").replace(" ", "_")
    results = []

    configs = {
        "25": ("ACORD 25 – Certificate of Liability", ACORD25_FIELDS),
        "27": ("ACORD 27 – Evidence of Property",     ACORD27_FIELDS),
        "28": ("ACORD 28 – Evidence of Property",     ACORD28_FIELDS),
        "30": ("ACORD 30 – Certificate of Garage",    ACORD30_FIELDS),
    }

    for num in forms:
        template = FORM_PATHS.get(num)
        if not template or not Path(template).exists():
            print(f"  ✗ Template not found: {template}")
            continue

        label, fmap = configs[num]
        if not fmap:
            print(f"  ⚠ {label}: field mapping not yet configured — skipping")
            continue

        out = Path(output_dir) / f"acord{num}_{insured}.pdf"
        print(f"\n  Filling {label}...")
        results.append(fill_pdf(template, str(out), fmap, data))

    return results


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python acord_filler.py <extracted_data.json> [--forms 25 27 28] [--outdir output/]")
        sys.exit(1)

    json_path = sys.argv[1]
    data = json.loads(Path(json_path).read_text())

    forms = None
    outdir = "output"
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--forms":
            forms = []
            i += 1
            while i < len(args) and not args[i].startswith("--"):
                forms.append(args[i])
                i += 1
        elif args[i] == "--outdir":
            i += 1
            outdir = args[i]
            i += 1
        else:
            i += 1

    insured = data.get("insured", {}).get("name", "Unknown")
    print(f"\n{'='*60}")
    print(f"  ACORD Form Filler")
    print(f"  Insured: {insured}")
    print(f"  Forms:   {', '.join(forms or determine_forms(data))}")
    print(f"  Output:  {outdir}/")
    print(f"{'='*60}")

    results = fill_acord_forms(data, outdir, forms)

    print(f"\n{'='*60}")
    print(f"  Done! {len(results)} form(s) filled.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()