#!/usr/bin/env python3
"""
main_az104_adapter.py (GitHub Actions Edition)

Changes:
- Added --lookup argument to accept the Image URL map from the Miner step.
- Updated MediaURL logic to prioritize Cloudflare URLs over the "1" flag.
- Configured for 14-column input schema.
"""

import argparse
import re
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set

# --- CONFIGURATION ---
EXPECTED_14_COLS = [
    "Question", "Options", "Question_Type", "has_image", "Correct Options",
    "Explanation", "Hints", "Category", "Collection", "Quiz",
    "Tag", "difficulty", "isPublic", "Status"
]
DEFAULT_CATEGORY_NAME = "MICROSOFT"
DEFAULT_COLLECTION_NAME = "Microsoft Azure"
DEFAULT_PASSMARK = 70
DEFAULT_POINTS = 1

# --------------------- HELPERS ---------------------

def slugify(text: str) -> str:
    tokens = re.sub(r"[^A-Za-z0-9]+", " ", str(text)).strip().split()
    return "-".join(t.upper() for t in tokens)

def make_key(prefix: str, base: str) -> str:
    return f"{prefix}-{slugify(base)}" if base else prefix

def sentence_case_name(name: str) -> str:
    return str(name).strip().title() if name else ""

def learning_outcome_for(collection_name: str) -> str:
    name = (collection_name or '').strip()
    table = {
        "Microsoft Azure": "Develop administrator-level skills in Azure identity, governance, storage, compute, networking, and monitoring aligned to AZ-104 objectives.",
        "Microsoft 365": "Build proficiency in Microsoft 365 services, security, compliance, and identity to manage modern workplace scenarios.",
        "Azure Data": "Strengthen data engineering and analytics skills on Azure services including storage, compute, security, and monitoring."
    }
    if name in table: return table[name]
    return f"Build proficiency in {name} aligned to relevant certification objectives." if name else ""

def clean_hint_text(h: str) -> str:
    if pd.isna(h) or h is None: return ""
    s = str(h).strip()
    s = re.sub(r'^\s*hint\s*:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(rf'\s*\[[0-9A-Za-z]{{3,8}}\]\s*$', '', s).strip()
    return s

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    k = lambda s: re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())
    aliases = {
        "questiontype": "Question_Type", "hasimage": "has_image",
        "correctoptions": "Correct Options", "correctoption": "Correct Options",
        "correctanswer": "Correct Options", "answers": "Correct Options",
        "tags": "Tag", "ispublic": "isPublic"
    }
    colmap = {k(c): c for c in df.columns}
    renames = {}
    
    for target in EXPECTED_14_COLS:
        kk = k(target)
        if kk in colmap: renames[colmap[kk]] = target
        
    for c in df.columns:
        kk = k(c)
        if c not in renames and kk in aliases: renames[c] = aliases[kk]
        
    df2 = df.rename(columns=renames)
    
    for col in EXPECTED_14_COLS:
        if col not in df2.columns: df2[col] = None
        
    return df2[EXPECTED_14_COLS]

def ensure_required_metadata(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Category"] = out.get("Category", DEFAULT_CATEGORY_NAME).fillna(DEFAULT_CATEGORY_NAME).astype(str).str.upper()
    out["Collection"] = out.get("Collection", DEFAULT_COLLECTION_NAME).fillna(DEFAULT_COLLECTION_NAME)
    out["Tag"] = out.get("Tag", "").fillna("")
    out["isPublic"] = out.get("isPublic", True).apply(lambda v: True if pd.isna(v) else bool(v) if isinstance(v, bool) else str(v).strip().lower() in {"true", "1", "yes", "y"})
    out["Status"] = "draft"
    out["difficulty"] = (out.get("difficulty", "medium").fillna("medium").astype(str).str.lower().apply(lambda s: s if s in {"low", "medium", "high"} else "medium"))
    out["Question_Type"] = out.get("Question_Type", "multiple_choice").fillna("multiple_choice").astype(str).str.lower()
    out["Hints"] = out.get("Hints", "").apply(clean_hint_text)
    
    if "has_image" not in out.columns:
        out["has_image"] = False
    else:
        out["has_image"] = out["has_image"].apply(lambda v: False if pd.isna(v) else bool(v) if isinstance(v, bool) else str(v).strip().lower() in {"true", "1", "yes", "y"})
    return out

def enforce_batches(df: pd.DataFrame, batch_size: int = 45) -> pd.DataFrame:
    # Logic is mainly handled in n8n now, but this acts as a fallback
    d = df.copy()
    if d["Quiz"].isna().any() or (d["Quiz"].astype(str).str.strip() == "").any():
        total = len(d)
        d["Quiz"] = [f"Batch {i//batch_size+1}" if pd.isna(q) or str(q).strip()=="" else q for i, q in enumerate(d["Quiz"])]
    return d

def split_options(text: str) -> List[str]:
    if pd.isna(text) or not str(text).strip(): return []
    s = str(text).strip()
    if re.search(r"\b[A-Ja-j]\)", s):
        parts = re.split(r"\s*[;|]\s*(?=[A-Ja-j]\))", s)
        cleaned = [re.sub(r"^[A-Ja-j]\)\s*", "", p).strip().strip(";") for p in parts if p.strip()]
        return [c for c in cleaned if c]
    parts = re.split(r"\s*[;|]\s*", s)
    return [p.strip() for p in parts if p.strip()]

def extract_correct_letters(correct: str) -> Set[str]:
    if pd.isna(correct) or not str(correct).strip(): return set()
    s = str(correct)
    letters = set(re.findall(r"\b([A-Ja-j])\b", s))
    return {x.upper() for x in letters}

def determine_type(qtype: str, options: List[str]) -> str:
    qtype = (qtype or "").strip().lower()
    if qtype in {"multiple_choice", "multiple answer", "multiple_answer", "true_false", "text_input", "short_answer", "image_based"}:
        return "multiple_answer" if qtype == "multiple answer" else qtype
    if len(options) == 2 and all(o.lower() in {"true", "false", "yes", "no"} for o in options):
        return "true_false"
    return "multiple_choice" if options else "text_input"

def load_agent_input(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        xls = pd.ExcelFile(path)
        sheet = "Extraction Template" if "Extraction Template" in xls.sheet_names else xls.sheet_names[0]
        df = pd.read_excel(path, sheet_name=sheet)
    elif suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix == ".json":
        with open(path, "r", encoding="utf-8") as f:
            df = pd.DataFrame(json.load(f))
    else:
        raise ValueError(f"Unsupported input type: {suffix}")
    
    df = normalize_columns(df)
    df = df.loc[~df["Question_Type"].astype(str).str.contains(r"hotspot|drag|simulation", case=False, na=False)].copy()
    df = ensure_required_metadata(df)
    df = enforce_batches(df, 45)
    return df

def build_key_maps(df: pd.DataFrame):
    cats = {str(x).strip(): make_key("CAT", x) for x in df["Category"].fillna(DEFAULT_CATEGORY_NAME).unique()}
    cols = {str(x).strip(): make_key("COL", x) for x in df["Collection"].fillna(DEFAULT_COLLECTION_NAME).unique()}
    quizzes = {str(x).strip(): make_key("QUIZ", x if str(x).strip() else "BATCH") for x in df["Quiz"].fillna("").unique()}
    return cats, cols, quizzes

def build_categories_df(cat_map: Dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame([{"CategoryKey": key, "Name": name, "Description": f"{sentence_case_name(name)} certification"} for name, key in cat_map.items()])

def build_collections_df(col_map: Dict[str, str], default_category_key: str) -> pd.DataFrame:
    rows = []
    for name, key in col_map.items():
        learning_outcome = learning_outcome_for(name)
        rows.append({
            "CollectionKey": key, "Name": name, "Description": f'{name}.',
            "LearningOutcome": learning_outcome, "IsPublic": True,
            "CategoryKey": default_category_key, "CoverImage": ""
        })
    return pd.DataFrame(rows)

# ------------- Tag inference -------------
ROLE_HINT_WORDS = {"administrator", "admin", "developer", "security", "architect", "engineer", "data", "ai", "devops", "fundamentals", "associate", "expert"}
KEYWORD_TAG_MAP = {
    r"\b(azure\s*ad|entra)\b": "identity", r"\bconditional access\b": "conditional-access",
    r"\bmfa\b|\bmultifactor\b": "mfa", r"\brbac\b": "rbac", r"\bkey vault\b": "key-vault",
    r"\bmanaged identity\b": "managed-identity", r"\bpolicy\b": "policy", r"\bblueprint[s]?\b": "blueprints",
    r"\bblob\b|\bstorage account\b": "storage", r"\bcosmos db\b": "cosmosdb", r"\bsql (managed instance|database)\b": "sql",
    r"\bvirtual machine\b|\bvm\b|\bscale set\b": "compute", r"\baks\b|\bkubernetes\b": "containers",
    r"\bapp service\b": "app-service", r"\bvnet\b|\bvirtual network\b|\bsubnet\b|\bnsg\b|\bpeering\b": "networking",
    r"\bapplication gateway\b|\bappgw\b": "application-gateway", r"\bbastion\b": "bastion",
    r"\bdefender for cloud\b|\bsecurity center\b": "defender", r"\bmonitor\b|\blog analytics\b": "monitoring",
    r"\bsentinel\b": "sentinel", r"\bbackup\b|\brecovery services vault\b|\basr\b": "backup",
    r"\bfunctions?\b": "functions", r"\bevent hub[s]?\b": "event-hubs", r"\bservice bus\b": "service-bus",
}

def tokenize_to_tags(text: str) -> List[str]:
    if not text: return []
    s = str(text).lower()
    hits = []
    for pat, tag in KEYWORD_TAG_MAP.items():
        if re.search(pat, s): hits.append(tag)
    return hits

def tags_from_collection(collection_name: str) -> List[str]:
    if not collection_name: return []
    tokens = re.split(r"[^a-z0-9]+", collection_name.lower())
    return [t for t in tokens if t]

def tags_from_quiz_title(title: str) -> List[str]:
    if not title: return []
    s = title.lower()
    tags = []
    m = re.search(r"\b([a-z]{1,3}-\d{2,4})\b", s)
    if m: tags.append(m.group(1))
    for w in ROLE_HINT_WORDS:
        if re.search(rf"\b{re.escape(w)}\b", s): tags.append(w)
    tokens = re.split(r"[^a-z0-9]+", s)
    tags.extend([t for t in tokens if t in {"azure", "microsoft"}])
    return list(dict.fromkeys(tags))

def infer_tags_for_quiz(quiz_df: pd.DataFrame, collection_name: str, quiz_title: str, max_tags: int = 8) -> str:
    tag_list = []
    tag_list.extend(tags_from_collection(collection_name))
    tag_list.extend(tags_from_quiz_title(quiz_title))
    content = (quiz_df.get("Question", "").fillna("").astype(str) + " " + quiz_df.get("Explanation", "").fillna("").astype(str)).str.lower()
    score = {}
    for txt in content:
        for pat, tag in KEYWORD_TAG_MAP.items():
            if re.search(pat, txt): score[tag] = score.get(tag, 0) + 1
    top = sorted(score.items(), key=lambda kv: (-kv[1], kv[0]))
    for tag, _ in top: tag_list.append(tag)
    norm = []
    seen = set()
    for t in tag_list:
        t = re.sub(r"[^a-z0-9\-]+", "", t.lower()).strip("-")
        if t and t not in seen:
            seen.add(t)
            norm.append(t)
        if len(norm) >= max_tags: break
    return ", ".join(norm)

# --------------------- BUILDERS ---------------------

def build_quizzes_df(df: pd.DataFrame, quiz_map: Dict[str, str], collection_key: str) -> pd.DataFrame:
    rows = []
    for title in df["Quiz"].astype(str).unique().tolist():
        sub = df.loc[df["Quiz"].astype(str) == title].copy()
        vc = sub["difficulty"].astype(str).value_counts(dropna=True)
        if len(vc) == 0: diff = "medium"
        else:
            top = vc.iloc[0]
            top_vals = vc[vc == top].index.tolist()
            diff = top_vals[0] if len(top_vals) == 1 else "medium"

        collection_name = sub["Collection"].iloc[0] if "Collection" in sub.columns and len(sub) > 0 else DEFAULT_COLLECTION_NAME
        exam_code = ""
        m = re.search(r"\b([a-z]{1,3}-\d{2,4})\b", title.lower())
        if m: exam_code = f" ({m.group(1).upper()})"
        
        quiz_description = f"{collection_name}"
        tags = infer_tags_for_quiz(sub, collection_name, title, max_tags=8)

        rows.append({
            "QuizKey": quiz_map[title.strip()],
            "CollectionKey": collection_key,
            "Title": title,
            "Description": quiz_description,
            "PassMark": DEFAULT_PASSMARK,
            "Difficulty": diff,
            "isPublic": True,
            "Status": "draft",
            "Tags": tags
        })
    return pd.DataFrame(rows)

# --- MODIFIED: Added image_lookup argument ---
def build_questions_and_options(df: pd.DataFrame, quiz_map: Dict[str, str], image_lookup: Dict[str, str]):
    q_rows = []
    o_rows = []
    counters = {}
    
    for _, r in df.iterrows():
        quiz_title = str(r.get("Quiz", "")).strip()
        quiz_key = quiz_map.get(quiz_title, make_key("QUIZ", quiz_title))
        
        counters.setdefault(quiz_key, 0)
        counters[quiz_key] += 1
        idx = counters[quiz_key]
        qkey = f"Q-{quiz_key}-{idx:03d}"
        
        # --- MEDIA URL LOGIC (Look up > Fallback) ---
        q_text = str(r.get("Question", ""))
        has_img = r.get("has_image", False)
        is_flagged = str(has_img).lower() in {'true', '1'}
        
        media_val = ""
        
        # 1. Check lookup map (Priority 1)
        if q_text in image_lookup:
            media_val = image_lookup[q_text]
        # 2. Fallback if flagged but no URL found (Priority 2)
        elif is_flagged:
            media_val = "1"
        
        options = split_options(r.get("Options", ""))
        qtype = determine_type(r.get("Question_Type", ""), options)
        
        correct_text = r.get("Correct Options", "")
        correct = set()
        
        if qtype == "multiple_choice":
            if pd.isna(correct_text) or not str(correct_text).strip():
                correct = set()
            else:
                s = str(correct_text).strip()
                match = re.search(r"^([A-Ja-j])\)", s)
                if match: correct = {match.group(1).upper()}
                
        elif qtype == "multiple_answer":
            if pd.isna(correct_text) or not str(correct_text).strip():
                correct = set()
            else:
                s = str(correct_text).strip()
                letters = re.findall(r"(?:^|;\s*)([A-Ja-j])\)", s)
                correct = {l.upper() for l in letters}
        else:
            correct = extract_correct_letters(correct_text)
        
        q_rows.append({
            "QuestionKey": qkey,
            "QuizKey": quiz_key,
            "Type": qtype,
            "Text": q_text,
            "Points": DEFAULT_POINTS,
            "Explanation": r.get("Explanation", ""),
            "Hints": clean_hint_text(r.get("Hints", "")),
            "MediaURL": media_val, # Holds URL or "1"
            "OrderIndex": idx,
            "ThresholdKeywords": ""
        })
        
        for i, opt in enumerate(options, start=1):
            letter = chr(ord("A") + i - 1)
            is_correct = letter in correct
            o_rows.append({
                "OptionKey": f"OPT-{qkey}-{i:02d}",
                "QuestionKey": qkey,
                "Text": opt,
                "IsCorrect": bool(is_correct),
                "OrderIndex": i,
                "CorrectOrder": "",
                "Meta": ""
            })
            
        if not options and qtype == "true_false":
            for i, opt in enumerate(["True", "False"], start=1):
                letter = "A" if i == 1 else "B"
                is_correct = letter in correct
                o_rows.append({
                    "OptionKey": f"OPT-{qkey}-{i:02d}",
                    "QuestionKey": qkey,
                    "Text": opt,
                    "IsCorrect": bool(is_correct),
                    "OrderIndex": i,
                    "CorrectOrder": "",
                    "Meta": ""
                })
                
    return pd.DataFrame(q_rows), pd.DataFrame(o_rows)

def read_template_schemas(template_path: Path):
    xls = pd.ExcelFile(template_path)
    needed = ["Categories", "Collections", "Quizzes", "Questions", "Options"]
    schemas = {}
    for s in needed:
        df = pd.read_excel(template_path, sheet_name=s)
        schemas[s] = list(df.columns)
    return schemas

def coerce(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns: df[c] = None
    return df[cols]

def save_to_workbook(out_path: Path, cats, cols, quizzes, questions, options, schemas):
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        coerce(cats, schemas["Categories"]).to_excel(w, "Categories", index=False)
        coerce(cols, schemas["Collections"]).to_excel(w, "Collections", index=False)
        coerce(quizzes, schemas["Quizzes"]).to_excel(w, "Quizzes", index=False)
        coerce(questions, schemas["Questions"]).to_excel(w, "Questions", index=False)
        coerce(options, schemas["Options"]).to_excel(w, "Options", index=False)

# --------------------- MAIN ---------------------

def main():
    parser = argparse.ArgumentParser(description="Convert agent output to SQ template workbook.")
    parser.add_argument("--input", required=True, help="Path to input Excel/JSON")
    parser.add_argument("--template", required=True, help="Path to SQ Template")
    parser.add_argument("--output", required=True, help="Path to output Excel")
    parser.add_argument("--lookup", required=False, help="Path to image_lookup.json")
    
    args = parser.parse_args()
    
    # 1. Load Data
    df = load_agent_input(Path(args.input))
    
    # 2. Load Lookup Map
    image_lookup = {}
    if args.lookup:
        try:
            with open(args.lookup, 'r', encoding='utf-8') as f:
                image_lookup = json.load(f)
            print(f"Loaded {len(image_lookup)} image URLs from lookup file.")
        except Exception as e:
            print(f"Warning: Could not load lookup file: {e}")

    # 3. Build Structures
    cat_map, col_map, quiz_map = build_key_maps(df)
    cats_df = build_categories_df(cat_map)
    cols_df = build_collections_df(col_map, default_category_key=list(cat_map.values())[0])
    quizzes_df = build_quizzes_df(df, quiz_map, collection_key=list(col_map.values())[0])
    
    # Pass lookup to questions builder
    questions_df, options_df = build_questions_and_options(df, quiz_map, image_lookup)
    
    # 4. Save
    schemas = read_template_schemas(Path(args.template))
    save_to_workbook(Path(args.output), cats_df, cols_df, quizzes_df, questions_df, options_df, schemas)
    
    print("Transformation Complete ✓")
    print(f"Total Questions: {len(df)}")
    print(f"Batches: {df['Quiz'].nunique()}")
    print(f"Output: {args.output}")

if __name__ == "__main__":
    main()
