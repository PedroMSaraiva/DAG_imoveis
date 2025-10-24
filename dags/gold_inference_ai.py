#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GOLD INFERENCE — IA de Classificação de Imóveis
Versão compacta para Airflow DAG (RTX 3050 friendly)
"""
from __future__ import annotations
import os, re, json, hashlib
from pathlib import Path
import pandas as pd
import numpy as np
from PIL import Image, ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True
import requests
from requests.adapters import HTTPAdapter, Retry
import torch, timm

# ======== CONFIG ========
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR                      # onde está anuncios.csv
OUTPUT_DIR = BASE_DIR.parent / "outputs"
IMAGES_DIR = OUTPUT_DIR / "imagens"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ======== UTILS ========
def requests_session():
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "Mozilla/5.0 (IA-Classificacao-Imoveis/1.0)"})
    return s

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def parse_imgs(val):
    if isinstance(val, list):
        return val
    if not isinstance(val, str) or not val.strip():
        return []
    try:
        j = json.loads(val)
        if isinstance(j, list):
            return [str(x) for x in j]
    except Exception:
        pass
    return [p.strip() for p in re.split(r"[;,\s]+", val) if p.strip()]

def download_img(url, folder: Path):
    folder.mkdir(parents=True, exist_ok=True)
    fpath = folder / (sha1(url) + ".jpg")
    if fpath.exists() and fpath.stat().st_size > 0:
        return fpath
    try:
        s = requests_session()
        r = s.get(url, timeout=10, stream=True)
        if r.status_code == 200:
            with open(fpath, "wb") as f:
                for c in r.iter_content(1 << 14):
                    if c:
                        f.write(c)
            return fpath
    except Exception:
        pass
    return None

# ======== HELPERS DE ID PARA SUBMISSÃO ========
import typing as _t

def only_digits(s: _t.Any) -> bool:
    s = str(s)
    return s.isdigit() and len(s) >= 6  # ids de anúncio costumam ter 8–12+ dígitos

def extract_numeric_id_from_url(url: str) -> _t.Optional[str]:
    """Pega a ÚLTIMA sequência longa de dígitos da URL (ex.: .../imovel-2811629951)
    Retorna None se não encontrar nada confiável.
    """
    if not isinstance(url, str):
        return None
    m = re.findall(r"(\d{6,})", url)
    if not m:
        return None
    return m[-1]

def compute_submission_id(row: pd.Series) -> _t.Optional[str]:
    """
    Regra:
      1) se id_anuncio já é numérico, usa.
      2) senão tenta extrair da URL.
      3) senão tenta id_imovel (se numérico).
      4) senão None.
    """
    ida = row.get("id_anuncio")
    if only_digits(ida):
        return str(ida)

    url = row.get("url") or row.get("link") or ""
    num_from_url = extract_numeric_id_from_url(str(url))
    if only_digits(num_from_url):
        return num_from_url

    idi = row.get("id_imovel")
    if only_digits(idi):
        return str(idi)

    return None


# ======== CSV ROBUSTO ========
def read_csv_any(path: Path) -> pd.DataFrame:
    """
    Lê CSV com detecção de encoding e separador (Windows-friendly).
    Garante as colunas mínimas: id_imovel,id_anuncio,imagens,titulo,descricao
    """
    must_have = {"id_imovel", "id_anuncio", "imagens", "titulo", "descricao"}
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    seps = [None, ",", ";", "\t", "|"]  # None => autodetect (engine='python')

    # tenta chardet (opcional)
    try:
        import chardet  # type: ignore
        with open(path, "rb") as fb:
            raw = fb.read(2_000_000)
        guess = chardet.detect(raw).get("encoding")
        if guess:
            encodings = [guess] + [e for e in encodings if e.lower() != str(guess).lower()]
    except Exception:
        pass

    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(
                    path,
                    encoding=enc,
                    sep=sep,
                    engine="python",     # permite sep=None (sniffer) e formatos irregulares
                    on_bad_lines="skip", # pandas <2.0 aceita; em >=2.0, mantém compatível
                )
                # normaliza nomes
                df.columns = [str(c).strip() for c in df.columns]
                # mapear variações (ex.: "Descrição" com acento)
                colmap = {}
                for c in df.columns:
                    cl = c.lower()
                    if cl in must_have:
                        colmap[c] = cl
                    elif cl == "descrição":
                        colmap[c] = "descricao"
                if colmap:
                    df = df.rename(columns=colmap)

                missing = must_have - set(df.columns)
                if missing:
                    low_map = {c: c.lower() for c in df.columns}
                    dflow = df.rename(columns=low_map)
                    missing2 = must_have - set(dflow.columns)
                    if not missing2:
                        df = dflow
                    else:
                        raise ValueError(f"Colunas ausentes: {missing2}")

                print(f"[read_csv_any] OK encoding='{enc}' sep='{sep or 'auto'}'  linhas={len(df)}")
                return df
            except Exception as e:
                last_err = e
                continue
    raise last_err if last_err else RuntimeError("Falha ao ler CSV")

# ======== MODELO VISUAL ========
MODEL_NAME = "vit_base_patch16_224.augreg_in21k"  # cabe na 3050
model = timm.create_model(MODEL_NAME, pretrained=True, num_classes=0).to(DEVICE).eval()
data_cfg = timm.data.resolve_model_data_config(model)
tfm = timm.data.create_transform(**data_cfg, is_training=False)

@torch.no_grad()
def embed_img(pil: Image.Image) -> np.ndarray:
    t = tfm(pil.convert("RGB")).unsqueeze(0).to(DEVICE)
    e = model(t).cpu().numpy()[0]
    return e / (np.linalg.norm(e) + 1e-9)

# ======== REGRAS DE TEXTO ========
PROMPTS = {
    "piso": ["Especial / Porcelanato", "Cerâmica", "Taco", "Cimento"],
    "forro": ["Especial", "Laje", "Gesso Simples / Pvc", "Sem"],
    "estrutura": ["Concreto", "Alvenaria", "Mista"],
    "esquadrias": ["Alumínio", "Ferro", "Especial", "Sem"],
    "revestimento_interno": ["Especial", "Material Cerâmico", "Massa", "Reboco"],
    "revestimento_externo": ["Especial", "Massa", "Reboco"],
    "cobertura": ["Especial", "Laje", "Telha de Barro", "Fibrocimento", "Zinco"],
    "benf": ["piscina", "churrasqueira", "espaço gourmet", "salão de festas", "portaria 24 horas"],
}

TEXT_SYNONYMS = {
    "piso:Especial / Porcelanato": ["porcelanato", "polido"],
    "piso:Cerâmica": ["cerâmica", "ceramico"],
    "forro:Especial": ["sanca", "rebaixado", "iluminação embutida"],
    "benf:piscina": ["piscina"],
    "benf:churrasqueira": ["churrasqueira"],
    "benf:espaço gourmet": ["espaço gourmet", "área gourmet", "area gourmet"],
    "benf:salão de festas": ["salão de festas", "salão festas"],
    "benf:portaria 24 horas": ["portaria 24h", "portaria vinte e quatro horas"],
}

CHAR_WEIGHTS = {
    "bias": 13.14,
    "piso": {"Especial / Porcelanato": 5.74, "Cerâmica": 4.54, "Taco": 2.05, "Cimento": 0.82},
    "forro": {"Especial": 5.87, "Laje": 4.43, "Gesso Simples / Pvc": 2.01, "Sem": 0.82},
    "estrutura": {"Concreto": 5.41, "Mista": 4.64, "Alvenaria": 3.09},
    "esquadrias": {"Especial": 5.17, "Alumínio": 4.39, "Ferro": 2.11, "Sem": 1.47},
    "revestimento_interno": {"Especial": 6.04, "Material Cerâmico": 3.99, "Massa": 2.29, "Reboco": 0.82},
    "revestimento_externo": {"Especial": 8.58, "Massa": 3.74, "Reboco": 0.82},
    "cobertura": {"Especial": 3.81, "Laje": 3.05, "Telha de Barro": 2.99, "Fibrocimento": 1.82, "Zinco": 1.47},
}
BENF_MAP = {
    "piscina": 1, "churrasqueira": 1, "espaço gourmet": 1, "salão de festas": 1, "portaria 24 horas": 1
}

def hits(text: str) -> set[str]:
    t = (text or "").lower()
    out = set()
    for k, ws in TEXT_SYNONYMS.items():
        if any(w in t for w in ws):
            out.add(k)
    return out

def decide_nominal(attr: str, options: list[str], hits_set: set[str]) -> str:
    for o in options:
        if f"{attr}:{o}" in hits_set:
            return o
    return options[0]  # default

def decide_benfs(hits_set: set[str]) -> list[str]:
    return sorted({k.split(":", 1)[1] for k in hits_set if k.startswith("benf:")})

def calc_score(pred: dict) -> dict:
    c = CHAR_WEIGHTS["bias"]
    for k in ["piso", "forro", "estrutura", "esquadrias", "revestimento_interno", "revestimento_externo", "cobertura"]:
        c += CHAR_WEIGHTS[k][pred[k]]
    b = sum(BENF_MAP.get(x, 0) for x in pred["benfeitorias"])
    pred["pontuacao_caracteristicas"] = round(c)
    pred["pontuacao_benfeitorias"] = int(b)
    pred["pontuacao_total"] = int(c + b)
    return pred

@torch.no_grad()
def predict_row(row: pd.Series) -> dict:
    text = f"{row.get('titulo', '')}\n{row.get('descricao', '')}"
    h = hits(text)

    # imagens (até 3)
    imgs = parse_imgs(row.get("imagens", ""))
    embs = []
    for u in imgs[:3]:
        if u.startswith("http"):
            p = download_img(u, IMAGES_DIR / str(row.get("id_imovel", "unk")))
        else:
            p = Path(u)
        if p and p.exists():
            try:
                with Image.open(p) as im:
                    embs.append(embed_img(im))
            except Exception:
                pass
    # (emb é opcional; por ora usamos só regras de texto)
    pred = {k: decide_nominal(k, PROMPTS[k], h) for k in PROMPTS if k != "benf"}
    pred["benfeitorias"] = decide_benfs(h)
    return calc_score(pred)

def run_pipeline():
    anuncios_csv = DATA_DIR / "anuncios.csv"
    out_parquet = OUTPUT_DIR / "anuncios_resultados.parquet"
    out_sub = OUTPUT_DIR / "submission.csv"

    df = read_csv_any(anuncios_csv)
    outs = []
    for i, r in df.iterrows():
        try:
            pred = predict_row(r)
            outs.append({
                "id_imovel": r.get("id_imovel"),
                "id_anuncio": r.get("id_anuncio"),
                **pred
            })
        except Exception as e:
            outs.append({"id_imovel": r.get("id_imovel"), "id_anuncio": r.get("id_anuncio"), "erro": str(e)})

        if (i + 1) % 25 == 0:
            print(f"[progress] {i+1}/{len(df)} anúncios processados")

    out = pd.DataFrame(outs)

    # salva GOLD (parquet)
    try:
        import pyarrow  # noqa: F401
        out.to_parquet(out_parquet, index=False)
        print(f"💾 Parquet salvo em: {out_parquet}")
    except Exception:
        fallback_csv = OUTPUT_DIR / "anuncios_resultados.csv"
        out.to_csv(fallback_csv, index=False)
        print(f"⚠️  Sem engine Parquet. Salvei CSV em: {fallback_csv}")

    # === SUBMISSÃO: garantir id_anuncio numérico ===
    out["id_submit"] = out.apply(compute_submission_id, axis=1)

    n_total = len(out)
    n_ok = out["id_submit"].notna().sum()
    if n_ok < n_total:
        falhas = out.loc[out["id_submit"].isna(), ["id_anuncio", "url", "id_imovel"]].head(10)
        print(f"⚠️  {n_total - n_ok} linhas sem id_submit válido (mostrando até 10):")
        print(falhas.to_string(index=False))

    sub = out.loc[out["id_submit"].notna(), ["id_submit", "pontuacao_total"]].copy()
    sub = sub.rename(columns={"id_submit": "id_anuncio"})

    # converter tipos
    sub["pontuacao_total"] = pd.to_numeric(sub["pontuacao_total"], errors="coerce").astype("Int64")

    dups = sub["id_anuncio"].duplicated().sum()
    if dups:
        print(f"⚠️  Encontradas {dups} chaves duplicadas em id_anuncio. Mantendo a primeira ocorrência.")
        sub = sub.drop_duplicates(subset=["id_anuncio"], keep="first")

    sub.to_csv(out_sub, index=False)
    print("✅ Submissão Kaggle salva em:", out_sub)


if __name__ == "__main__":
    run_pipeline()