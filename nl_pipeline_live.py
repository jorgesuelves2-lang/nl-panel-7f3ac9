#!/usr/bin/env python3
"""Refresco LIGERO del pipeline (cada 15 min): solo etapas de los leads vivos + último mensaje.
Escribe pipeline_live.json; la pestaña Pipeline del dashboard lo lee por fetch(). No regenera
nada más: el histórico pesado lo lleva el refresco horario (dashboard_pipeline.py).
~10-15 peticiones a GHL por pasada, en minutos separados del resto de motores."""
import subprocess, json, os, re, time, datetime

def env(k):
    v = os.environ.get(k)
    if v: return v
    b = open(os.path.expanduser("~/.natscholibre_secrets/ghl.env")).read()
    return re.search(rf'{k}=(.+)', b).group(1).strip()

TOKEN = env("GHL_TOKEN"); LOC = env("GHL_LOCATION_ID")
H21 = ["-H", f"Authorization: Bearer {TOKEN}", "-H", "Version: 2021-07-28", "-H", "Accept: application/json"]
LEADS_PIPE = "mW4ZfvQnRARhIlgmpj6e"   # pipeline "LEADS"

def _etapa_viva(n):
    n = (n or "").lower()
    return not any(x in n for x in ("no show", "no confirma", "no cualifica", "pago completo", "reembolso", "descartado"))

def cg(url):
    # mismo guardián que el pipeline grande: GHL responde al rate-limit con JSON válido y lista vacía
    for a in range(5):
        r = subprocess.run(["curl", "-sg", "-m", "25", url, *H21], capture_output=True, text=True).stdout
        if r:
            try: d = json.loads(r)
            except Exception: d = None
            if isinstance(d, dict):
                sc = d.get("statusCode") or d.get("status")
                msg = str(d.get("message") or d.get("error") or "").lower()
                malo = False
                try: malo = bool(sc) and int(sc) >= 400
                except Exception: malo = False
                if malo or "too many" in msg or "rate limit" in msg:
                    time.sleep(2.0 * (a + 1)); continue
                return d
        time.sleep(0.5 * (a + 1))
    return {}

def dms(ms):
    return datetime.datetime.utcfromtimestamp(ms / 1000).strftime('%Y-%m-%d') if ms else ""

# 1) etapas del pipeline LEADS
stages = {}
for p in cg(f"https://services.leadconnectorhq.com/opportunities/pipelines?locationId={LOC}").get("pipelines", []):
    if p.get("id") == LEADS_PIPE:
        for s in p.get("stages", []): stages[s["id"]] = s.get("name")
if not stages:
    raise SystemExit("sin etapas: GHL no respondió, no se escribe nada (se conserva el archivo anterior)")

# 2) oportunidades abiertas del pipeline LEADS en etapas vivas
leads = {}
url = f"https://services.leadconnectorhq.com/opportunities/search?location_id={LOC}&limit=100&status=open"
n = 0
while url and n < 8:
    d = cg(url)
    for o in d.get("opportunities", []):
        if o.get("pipelineId") != LEADS_PIPE: continue
        et = stages.get(o.get("pipelineStageId"), "")
        if not _etapa_viva(et): continue
        ct = o.get("contact") or {}
        cid = ct.get("id")
        if not cid: continue
        leads[cid] = {"etapa": et,
                      "desde": str(o.get("lastStageChangeAt") or o.get("createdAt") or "")[:10],
                      "nombre": ct.get("name") or "",
                      "ficha": f"https://app.funnelup.io/v2/location/{LOC}/contacts/detail/{cid}",
                      "ult": {}}
    url = ((d.get("meta") or {}).get("nextPageUrl")) or None
    n += 1
if not leads:
    raise SystemExit("0 leads vivos: casi seguro estrangulamiento; no se escribe nada")

# 3) último mensaje por contacto (primeras páginas del listado de conversaciones, lo más reciente)
sa = None
for _ in range(4):
    ps = f"locationId={LOC}&limit=100&sortBy=last_message_date&sort=desc" + (f"&startAfterDate={sa}" if sa else "")
    d = cg(f"https://services.leadconnectorhq.com/conversations/search?{ps}")
    cs = d.get("conversations", [])
    if not cs: break
    for c in cs:
        cid = c.get("contactId")
        if cid in leads and not leads[cid]["ult"]:
            leads[cid]["ult"] = {"fecha": dms(c.get("lastMessageDate") or 0),
                                 "dir": c.get("lastMessageDirection") or "",
                                 "txt": (c.get("lastMessageBody") or "")[:200],
                                 "tipo": c.get("lastMessageType") or ""}
    sa = cs[-1].get("lastMessageDate")

out = {"generado": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M'), "leads": leads}
json.dump(out, open("pipeline_live.json", "w"), ensure_ascii=False)
print(f"pipeline_live.json: {len(leads)} leads vivos | con último mensaje: {sum(1 for v in leads.values() if v['ult'])}")
