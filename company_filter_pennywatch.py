# --- company_filter_pennywatch.py ---
import difflib
import re
import logging

APPROVED_COMPANIES = [
    "Koninklijke BAM Groep N.V.",
    "Koninklijke Heijmans N.V.",
    "Wereldhave N.V.",
    "Pharming Group N.V.",
    "Acomo N.V.",
    "Nedap N.V.",
    "TomTom N.V.",
    "B&S Group S.A.",
    "PostNL N.V.",
    "Fastned B.V.",
    "Sligro Food Group N.V.",
    "Brunel International N.V.",
    "NSI N.V.",
    "ForFarmers N.V.",
    "Kendrion N.V.",
    "Sif Holding N.V.",
    "Accsys Technologies PLC",
    "NX Filtration N.V.",
    "Azerion Group N.V.",
    "CM.com N.V.",
    "Avantium N.V.",
    "Vivoryon Therapeutics N.V.",
    "Ebusco Holding N.V.",
    "Tetragon Financial Group Limited",
    "Retail Estates N.V.",
    "Envipco Holding N.V.",
    "Volta Finance Limited",
    "Hydratec Industries N.V.",
    "MotorK PLC",
    "AFC Ajax N.V.",
    "SPEAR Investments I B.V.",
    "The London Tunnels PLC",
    "Holland Colours N.V.",
    "Value8 N.V.",
    "Bever Holding N.V.",
    "Cabka N.V.",
    "Morefield Group N.V.",
    "Ctac N.V.",
    "New Amsterdam Invest N.V.",
    "Alumexx N.V.",
    "MKB Nedsense N.V.",
    "PB Holding N.V.",
    "Eurocastle Investment Limited",
    "Ease2pay N.V.",
    "B.V. Delftsch Aardewerkfabriek “De Porceleyne Fles Anno 1653”",
    "Green Earth Group N.V.",
    "Lavide Holding N.V.",
    "New Sources Energy N.V.",
    "Titan N.V.",
    "Almunda Professionals N.V.",
    "Agility Capital Holding Inc."
]

def normalize_name(name: str) -> str:
    """Clean and standardize company names for comparison."""
    if not name:
        return ""
    name = name.lower()
    # Remove punctuation and known suffixes
    name = re.sub(r'[^a-z0-9& ]', '', name)
    name = re.sub(r'\b(nv|bv|sa|plc|inc|group|holding|limited|nederland|the|royal)\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def is_approved_company(name: str) -> bool:
    """Return True if company name matches (fuzzy or partial) with approved list."""
    if not name:
        return False
    name_clean = normalize_name(name)
    for company in APPROVED_COMPANIES:
        comp_clean = normalize_name(company)
        # Direct or partial match
        if name_clean in comp_clean or comp_clean in name_clean:
            return True
        # Fuzzy similarity match
        ratio = difflib.SequenceMatcher(None, name_clean, comp_clean).ratio()
        if ratio > 0.8:
            return True
    return False
