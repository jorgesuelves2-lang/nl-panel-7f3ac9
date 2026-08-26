#!/usr/bin/env python3
"""Pipeline del dashboard NatschoLibre. Descarga setting (conversaciones+mensajes, en paralelo) y triaje
(calendario), calcula KPIs por día y genera dashboard.html autocontenido. Para correr a diario."""
import subprocess, json, os, re, time, datetime, statistics
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter
def env(k):
    v=os.environ.get(k)
    if v: return v
    b=open(os.path.expanduser("~/.natscholibre_secrets/ghl.env")).read(); return re.search(rf'{k}=(.+)',b).group(1).strip()
TOKEN=env("GHL_TOKEN"); LOC=env("GHL_LOCATION_ID")
H=["-H",f"Authorization: Bearer {TOKEN}","-H","Version: 2021-04-15","-H","Accept: application/json"]
H21=["-H",f"Authorization: Bearer {TOKEN}","-H","Version: 2021-07-28","-H","Accept: application/json"]
# OJO 18-ago-2026: hay DOS calendarios de triaje ACTIVOS en paralelo (el duplicado 2EY5 domina desde julio,
# pero 1kHW sigue recibiendo citas — ej. triaje de Olga Betancur 17-ago). Hay que leer los dos SIEMPRE.
TRIAGE_CALS=["2EY5mRYqpaAx4qfnsWJM","1kHWabxSmIJSHTfdr7s5"]; START="2026-02-01"  # fecha más antigua que se descarga
HERE=os.path.dirname(os.path.abspath(__file__))
OUTDIR=os.environ.get("OUTDIR","/Users/jorgesuelves/Desktop/Claude Code/marcas/natscholibre/dashboard")
os.makedirs(OUTDIR,exist_ok=True)
def cg(url,params=[],headers=H):
    # 24-ago-2026: GHL contesta al rate-limit con un JSON PERFECTAMENTE VALIDO (429 / "Too Many Requests").
    # Antes eso se daba por bueno, .get("events") devolvia [] y la seccion se publicaba VACIA sin avisar
    # (asi se borro la pestana de closing el 24-ago a las 13:11). Ahora se detecta y se reintenta.
    for a in range(5):
        r=subprocess.run(["curl","-s","-m","25","-G",url,*sum([["--data-urlencode",p] for p in params],[]),*headers],capture_output=True,text=True).stdout
        if r:
            try: d=json.loads(r)
            except Exception: d=None
            if isinstance(d,dict):
                _sc=d.get("statusCode") or d.get("status")
                _msg=str(d.get("message") or d.get("error") or "")
                _malo=False
                try: _malo=bool(_sc) and int(_sc)>=400
                except Exception: _malo=False
                if _malo or "too many" in _msg.lower() or "rate limit" in _msg.lower():
                    time.sleep(2.0*(a+1)); continue
                return d
            elif d is not None:
                return d
        time.sleep(0.4*(a+1))
    print("AVISO GHL: sin respuesta util tras 5 intentos ->",url[:90],flush=True)
    return {}
def dms(ms): return datetime.datetime.utcfromtimestamp(ms/1000).strftime('%Y-%m-%d') if ms else None
now=int(time.time()*1000)
_sdt=datetime.datetime.strptime(START,'%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
cutoff=int(_sdt.timestamp()*1000)
DAYS=(datetime.datetime.now(datetime.timezone.utc).date()-_sdt.date()).days+1
days=[(_sdt+datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(DAYS)]
LINK=re.compile(r'agendaor|agendaror|natscholibre\.com/agenda|ag[eé]ndame|agendar|calendario|te paso el (link|calendario)',re.I)
# Propuesta REAL = se envió el link de agenda. Distinguimos link BIEN enviado (snippet de GHL, con
# contact_id resuelto -> actualiza la ficha existente) de link ROTO (copiado a mano: sin contact_id o
# con {{contact.id}} literal -> el lead crea una ficha DUPLICADA al agendar).
AGLINK=re.compile(r'natscholibre\.com/(agenda[\w-]*|agendaror\d*)',re.I)
BADLINK=re.compile(r'natscholibre\.com/agenda[\w-]*(?![^\s]*contact_id=[A-Za-z0-9]{15})',re.I)
# --- CACHÉ de setting: los días pasados se CONGELAN (no se re-descargan 8000 conversaciones cada vez).
# Solo se recalculan los últimos RECOMPUTE_DAYS días. La primera vez (sin caché) = BACKFILL completo. ---
CACHE_PATH=os.path.join(OUTDIR,"setting_cache.json")
try: _cache=json.load(open(CACHE_PATH))
except Exception: _cache={}
_cache.setdefault("days",{}); _cache.setdefault("resp_pairs",{}); _cache.setdefault("horas",{}); _cache.setdefault("convs",{})
RECOMPUTE_DAYS=int(os.environ.get("RECOMPUTE_DAYS","12"))
BACKFILL=(not _cache["days"]) or os.environ.get("BACKFILL")=="1"
RECFROM=START if BACKFILL else (datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=RECOMPUTE_DAYS)).strftime('%Y-%m-%d')
RECFROM_TS=int(datetime.datetime.strptime(RECFROM,'%Y-%m-%d').replace(tzinfo=datetime.timezone.utc).timestamp()*1000)
print(("setting: BACKFILL completo" if BACKFILL else f"setting: incremental (recalcula desde {RECFROM})"),flush=True)

# ---- KPIs que reportan las setters en el formulario diario (fuente: submissions de GHL) ----
# Se leen del propio formulario, no del Sheet: así no hacen falta credenciales de Google aquí.
KPI_FORM="xYo17QaYXG2x9hFK7uyn"
KF={"fecha":"H2w4exowODqlxqsYeEBC","setter":"hJ0AUdYi22CUZnvb3WBT","fu":"VNCxDXhSvsZdcdW1Mphc",
    "inb":"zMslhPNgyrKp0pYYjG0l","out":"LR3a7VoTUFOMZaxFRt4z","prop":"xTmXwfh7ggLHG1bavHI4",
    "book":"5Wp6rzCeWSHDIk07Wy0D","wel":"MBVAn20XqrepyQHVjXxt","notas":"NHe4iggWwz8BSg9jh3Wt"}
def _n(v):
    try: return int(float(str(v).replace(",",".")))
    except: return 0
# ---- CUMPLIMIENTO: formulario post-triaje (se atribuye al DIA DE LA LLAMADA, no al del envio) ----
POST_FORM="hstHa9zweROKcpLZUepv"
post_by_cid={}; post_by_mail={}
try:
    _pg=1
    while True:
        _d=cg("https://services.leadconnectorhq.com/forms/submissions",
              [f"locationId={LOC}",f"formId={POST_FORM}","limit=100",f"page={_pg}"],H21)
        _subs=_d.get("submissions",[]) or []
        for s in _subs:
            o=s.get("others") or {}
            f=(s.get("createdAt") or "")[:10]
            if s.get("contactId"): post_by_cid.setdefault(s["contactId"],f)
            em=str(o.get("email") or s.get("email") or "").lower().strip()
            if em: post_by_mail.setdefault(em,f)
        if not (_d.get("meta") or {}).get("nextPage"): break
        _pg+=1
except Exception as _e:
    print("   [aviso] formulario post-triaje:",str(_e)[:70],flush=True)
print("   post-triaje: %d envios" % len(post_by_cid),flush=True)

kpis=[]
try:
    _pg=1
    while True:
        _d=cg("https://services.leadconnectorhq.com/forms/submissions",
              [f"locationId={LOC}",f"formId={KPI_FORM}","limit=100",f"page={_pg}"],H21)
        _subs=_d.get("submissions",[]) or []
 #      print("   [debug] pagina",_pg,"->",len(_subs),"submissions | claves resp:",list(_d.keys())[:4],flush=True)
        for s in _subs:
            o=s.get("others") or {}
            crudo=str(o.get(KF["setter"]) or "")
            nom=re.sub(r"^\s*\d+\s*-\s*","",crudo).strip().lower()
            setter={"sary":"Sary","sara":"Sara","sarisa":"Sara",
                    "jesmary":"Jesmary"}.get(nom)
            fecha=str(o.get(KF["fecha"]) or s.get("createdAt") or "")[:10]
            if not setter or not re.match(r"\d{4}-\d{2}-\d{2}",fecha): continue
            kpis.append({"dia":fecha,"setter":setter,"_env":str(s.get("createdAt") or ""),
                "fu":_n(o.get(KF["fu"])),"inb":_n(o.get(KF["inb"])),"out":_n(o.get(KF["out"])),
                "prop":_n(o.get(KF["prop"])),"book":_n(o.get(KF["book"])),"wel":_n(o.get(KF["wel"])),
                "notas":str(o.get(KF["notas"]) or "")})
        if not (_d.get("meta") or {}).get("nextPage"): break
        _pg+=1
    # correccion: si un setter envio 2 veces el mismo dia, vale SOLO el ultimo envio
    _ult={}
    for _k in kpis:
        _key=(_k["dia"],_k["setter"])
        if _key not in _ult or _k.get("_env","")>_ult[_key].get("_env",""): _ult[_key]=_k
    kpis=[{k:v for k,v in r.items() if k!="_env"} for r in _ult.values()]
    print("   kpis tras dedupe:",len(kpis),flush=True)
    print("KPIs reportados por setters:",len(kpis),flush=True)
except Exception as e:
    print("AVISO: no se pudieron leer los KPIs del formulario:",e,flush=True)

# ---- KOMMO: de quien es cada conversacion (etiquetas de setter) y de donde vino (origen) ----
# Kommo es el CRM del setting: cada chat es un lead con etiquetas ("Sary","Sara","Jes","Outbound frio","Ads"...).
# Se cruza por NOMBRE del contacto con las conversaciones de FunnelUp. Si Kommo no responde, el panel
# sigue funcionando y todo queda como "sin asignar" (nunca rompe).
import unicodedata as _ud
def _knorm(x):
    x=_ud.normalize("NFKD",(x or "")).encode("ascii","ignore").decode().lower()
    return " ".join(re.sub(r'[^a-z0-9 ]',' ',x).split())
KSET={}; KSET2={}; KSET3={}
try:
    _ktok=os.environ.get("KOMMO_TOKEN") or ""
    if not _ktok:
        _kp=os.path.expanduser("~/.natscholibre_secrets/kommo.env")
        if os.path.exists(_kp):
            _km=re.search(r'KOMMO_LONG_LIVED_TOKEN=(.+)',open(_kp).read())
            if _km: _ktok=_km.group(1).strip()
    if _ktok:
        def _kget(pth):
            # OJO: -g obligatorio (los corchetes de order[...] disparan el globbing de curl -> cuerpo vacio)
            o=subprocess.run(["curl","-sg","--compressed","-m","30",f"https://aprendealeman.kommo.com{pth}",
                "-H",f"Authorization: Bearer {_ktok}","-H","Accept: application/json"],capture_output=True,text=True).stdout
            try: return json.loads(o)
            except Exception: return {}
        _ltags={}
        _pg=1
        while _pg<40:
            _d=_kget(f"/api/v4/leads?limit=250&page={_pg}&with=contacts")
            _ls=(_d.get("_embedded") or {}).get("leads") or []
            if not _ls: break
            for _l in _ls:
                _tg=[(x.get("name") or "").lower().strip() for x in ((_l.get("_embedded") or {}).get("tags") or [])]
                _st=("Sary" if "sary" in _tg else
                     "Sara" if any(t=="sara" or t.startswith("sara ") for t in _tg) else
                     "Jesmary" if "jes" in _tg or "jesmary" in _tg else "")
                _or=next((t for t in _tg if t in ("outbound frio","ads","cta comentario","inbound","outbound seguidores","skool","poll historia")),"")
                if not (_st or _or): continue
                for _ct in ((_l.get("_embedded") or {}).get("contacts") or []):
                    if _ct.get("id"): _ltags.setdefault(_ct["id"],{"s":_st,"o":_or})
            _pg+=1
        _pg=1
        while _pg<80:
            _d=_kget(f"/api/v4/contacts?limit=250&page={_pg}")
            _cs=(_d.get("_embedded") or {}).get("contacts") or []
            if not _cs: break
            for _ct in _cs:
                _t=_ltags.get(_ct.get("id"))
                if _t:
                    _n=_knorm(_ct.get("name"))
                    if _n:
                        KSET.setdefault(_n,_t)
                        _p2=" ".join(_n.split()[:2])
                        if len(_n.split())>=2: KSET2.setdefault(_p2,_t)
                        _c3=re.sub(r'[^a-z0-9]','',_n)   # forma compacta: "dra.axia" == "dra axia"
                        if len(_c3)>=5: KSET3.setdefault(_c3,_t)
            _pg+=1
        print("kommo: contactos con setter/origen casables:",len(KSET),flush=True)
except Exception as _e:
    print("AVISO Kommo:",str(_e)[:80],flush=True)
def _canal_bucket(o):
    o=(o or "").lower()
    if "ads" in o: return "Ads"
    if "cta" in o: return "CTA comentario"
    if "outbound" in o: return "Outbound"
    if "inbound" in o: return "Inbound"
    return "Otros" if o else ""
def origen_de(nm):
    n=_knorm(nm)
    if not n: return ""
    t=(KSET.get(n)
       or (KSET2.get(" ".join(n.split()[:2])) if len(n.split())>=2 else None)
       or KSET3.get(re.sub(r'[^a-z0-9]','',n)))
    return _canal_bucket(t["o"]) if t else ""
def _canal_cta(v):
    v=(v or "").lower()
    if not v.strip(): return ""
    if "outbound" in v: return "Outbound"
    if "http" in v or "reel" in v or "instagram" in v: return "CTA comentario"
    if "anuncio" in v or re.search(r'\bads?\b',v): return "Ads"
    if "inbound" in v: return "Inbound"
    return "Otros"
def fuente_de(nombre,cta):
    return origen_de(nombre) or _canal_cta(cta)
def setter_de(nm):
    n=_knorm(nm)
    if not n: return ""
    t=(KSET.get(n)
       or (KSET2.get(" ".join(n.split()[:2])) if len(n.split())>=2 else None)
       or KSET3.get(re.sub(r'[^a-z0-9]','',n)))
    return t["s"] if t else ""

# 1) lista de conversaciones (setting = IG+FB)
base="https://services.leadconnectorhq.com/conversations/search"; convs=[]; sa=None; pages=0
while True:
    params=[f"locationId={LOC}","limit=100","sortBy=last_message_date","sort=desc"]
    if sa: params.append(f"startAfterDate={sa}")
    d=cg(base,params); cs=d.get("conversations",[])
    if not cs: break
    pages+=1; stop=False
    for c in cs:
        if (c.get("lastMessageDate") or 0)<RECFROM_TS: stop=True; break  # incremental: solo conversaciones activas en la ventana
        # NO filtramos por lastMessageType: un email reciente puede ocultar una conversación con DMs de IG.
        # Se filtra por canal a nivel de MENSAJE en fetch().
        convs.append(c)
    if stop or len(cs)<100 or pages>200: break
    sa=cs[-1].get("lastMessageDate")
print("conversaciones setting:",len(convs),flush=True)

def evs(cal,s,e):
    # Un 0 aqui no es "no hay citas", casi siempre es estrangulamiento: reintenta antes de rendirse.
    for _a in range(3):
        _ev=cg(f"https://services.leadconnectorhq.com/calendars/events?locationId={LOC}&calendarId={cal}&startTime={s}&endTime={e}",headers=H21).get("events",[])
        if _ev: return _ev
        time.sleep(1.5*(_a+1))
    print("AVISO: calendario",cal,"devolvio 0 eventos",flush=True)
    return []

# 1b) triaje + closing (calendario) PRIMERO, para que no le afecte el rate-limit de los mensajes
ev=[]
for _tc in TRIAGE_CALS:
    ev+=evs(_tc,cutoff,now)
t_status=defaultdict(Counter)
for e in ev: t_status[str(e.get("startTime"))[:10]][e.get("appointmentStatus","?")]+=1
# contactos que pasaron a CLOSING (calendarios de Planificación Estratégica)
endf=now+30*86400*1000; closing_contacts=set()
for cal in ["VRaGr4KGSZNiuDamyV4q","998ij1w7jUrmPqJZu43V"]:
    for e in evs(cal,cutoff,endf):
        if e.get("contactId"): closing_contacts.add(e["contactId"])
t_cual=defaultdict(int)
for e in ev:
    if e.get("contactId") and e["contactId"] in closing_contacts: t_cual[str(e.get("startTime"))[:10]]+=1
t_nocual=defaultdict(int); t_seg=defaultdict(int)  # se rellenan en el bucle de leads (etiquetas de resultado)
# FIX 25-jul: el show-up se deduce de EVIDENCIAS de que la llamada ocurrió, no del appointmentStatus
# (nadie marca "Asistió" en el calendario: se queda en 'confirmed' para siempre). Evidencias, por fiabilidad:
#   1) GRABACIÓN en Fathom con el nombre del lead  -> prueba directa de que la llamada se hizo
#   2) etiqueta de resultado del formulario post-triaje (cualifica / no-cualifica / seguimiento)
#   3) appointmentStatus == showed (respaldo, casi nunca se marca)
t_cualT=defaultdict(int); t_showT=defaultdict(int); t_noshT=defaultdict(int); t_nocfT=defaultdict(int)

# --- Fathom: nombres con grabación de triaje (evidencia nº1 de asistencia) ---
import unicodedata
def _nrm(s):
    s=unicodedata.normalize('NFKD',(s or '').lower()); s=''.join(c for c in s if not unicodedata.combining(c))
    s=re.sub(r'\b(ing|dr|dra|md|mg|med|odont|e-md|arg)\b','',s); return re.sub(r'[^a-z ]','',s).split()
def _nk(s): return " ".join(_nrm(s)[:2])
FATHOM_TRI=set(); FATHOM_CLO=set()   # 25-ago: closing tambien, para inferir asistencia a la llamada de venta
def _fkeys():
    # TODAS las cuentas de Fathom (Natalie/David + Christian...). 19-ago-2026: antes solo se leia
    # la principal, asi que los triajes de Christian no contaban como "asistido" en el panel.
    ks=[v.strip() for k,v in os.environ.items() if k.startswith("FATHOM_API_KEY") and (v or "").strip()]
    _p=os.path.expanduser("~/.natscholibre_secrets/fathom.env")
    if os.path.exists(_p):
        for _l in open(_p):
            _m=re.match(r'(FATHOM_API_KEY[A-Za-z0-9_]*)\s*=\s*(\S.*)',_l.strip())
            if _m: ks.append(_m.group(2).strip())
    seen=set(); out=[]
    for k in ks:
        if k and k not in seen: seen.add(k); out.append(k)
    return out
for _fk in _fkeys():
    try:
        _ca=_sdt.strftime('%Y-%m-%dT%H:%M:%SZ'); _cur=None; _f=0
        while True:
            _u=f'https://api.fathom.ai/external/v1/meetings?include_transcript=false&limit=50&created_after={_ca}'+(f'&cursor={_cur}' if _cur else '')
            _d=cg(_u,headers=["-H",f"X-Api-Key: {_fk}"])
            if "items" not in _d:
                _f+=1
                if _f>6: print("AVISO Fathom: mapa parcial",flush=True); break
                time.sleep(3); continue
            for _m in _d["items"]:
                _t=_m.get("title") or ""
                _es_clo=bool(re.search(r'closing|planificaci|estrateg',_t,re.I))
                _es_tri=(not _es_clo) and bool(re.search(r'triage|triaje|introducci|validaci',_t,re.I))
                if not (_es_clo or _es_tri): continue
                # 25-ago: antes 'dr\.?' casaba con el 'dr' DENTRO de Alejan-dra/Pe-dro/San-dra y
                # descartaba el nombre del lead (mismo bug corregido en el autofill el 24-ago).
                _KW=re.compile(r'reuni|introducci|validaci|triage|triaje|llamada|closing|planificaci|estrateg|\bdra?\.',re.I)
                _sg=[s.strip() for s in re.split(r'\s*-\s*',_t) if s.strip()]
                _ld=next((s for s in _sg if not _KW.search(s)),"")
                if _ld: (FATHOM_CLO if _es_clo else FATHOM_TRI).add(_nk(_ld))
            _cur=_d.get("next_cursor")
            if not _cur: break
    except Exception as _e:
        print("AVISO Fathom (una cuenta no disponible):",str(_e)[:80],flush=True)
print("grabaciones Fathom -> triajes:",len(FATHOM_TRI),"| closings:",len(FATHOM_CLO),flush=True)
print("eventos triaje:",len(ev),"| contactos con closing:",len(closing_contacts),flush=True)

# 1c) CAMINO DE LOS LEADS + CLOSING (desde START) — antes de los mensajes para evitar rate-limit
LJcut=cutoff
cids={}
for _tc in TRIAGE_CALS:
    for e in evs(_tc,LJcut,now):
        if e.get("contactId"):
            _i=cids.setdefault(e["contactId"],{})
            _i["tri"]=e
            _i.setdefault("tris",[]).append(e)   # todas las citas de triaje, no solo la ultima
for cal in ["VRaGr4KGSZNiuDamyV4q","998ij1w7jUrmPqJZu43V"]:
    for e in evs(cal,LJcut,endf):
        if e.get("contactId"):
            _i=cids.setdefault(e["contactId"],{})
            _i["clo"]=e                              # la ultima (compatibilidad con lo que ya existia)
            _i.setdefault("clos",[]).append(e)       # 23-ago-2026: TODAS las citas de closing del lead,
            # para que la pestana de closing pueda listar cada llamada de cada semana, no solo la ultima.
# --- ETAPA ACTUAL DEL PIPELINE por contacto (23-ago-2026) ---
# Se baja TODO el pipeline de una vez (paginado) en lugar de preguntar oportunidad por oportunidad:
# son ~250 llamadas menos y esta cuenta ya sufre rate-limit.
ETAPA_DE={}
# 25-ago: ademas del nombre de etapa, guardamos la oportunidad completa del pipeline "LEADS"
# (etapa, cuando entro en ella, estado) para la pestana Pipeline: leads vivos en el embudo.
LEADS_PIPE="mW4ZfvQnRARhIlgmpj6e"   # pipeline "LEADS" de FunnelUp
# etapa VIVA = no es un cierre (rojos/completados/reembolso). Se decide por el nombre, asi
# si crean una etapa nueva entra sola mientras no sea de cierre.
def _etapa_viva(n):
    n=(n or "").lower()
    return not any(x in n for x in ("no show","no confirma","no cualifica","pago completo","reembolso","descartado"))
OPPS={}; ETAPAS_ORDEN=[]
try:
    _stages={}; _pos={}
    for _p in cg(f"https://services.leadconnectorhq.com/opportunities/pipelines?locationId={LOC}",headers=H21).get("pipelines",[]):
        for _s in _p.get("stages",[]):
            _stages[_s["id"]]=_s.get("name")
            if _p.get("id")==LEADS_PIPE: _pos[_s.get("name")]=_s.get("position",99)
    ETAPAS_ORDEN=[n for n,_ in sorted(_pos.items(),key=lambda kv:kv[1]) if _etapa_viva(n)]
    _url=f"https://services.leadconnectorhq.com/opportunities/search?location_id={LOC}&limit=100"
    _n=0
    while _url and _n<15:
        _d=cg(_url,headers=H21)
        for _o in _d.get("opportunities",[]):
            _ct=_o.get("contact") or {}
            _c=_ct.get("id")
            if not _c: continue
            ETAPA_DE[_c]=_stages.get(_o.get("pipelineStageId"),"")
            if _o.get("pipelineId")==LEADS_PIPE and _o.get("status")=="open":
                _en=_stages.get(_o.get("pipelineStageId"),"")
                OPPS[_c]={"etapa":_en,"viva":_etapa_viva(_en),
                          "desde":str(_o.get("lastStageChangeAt") or _o.get("createdAt") or "")[:10],
                          "nombre":_ct.get("name") or "","email":_ct.get("email") or "","tel":_ct.get("phone") or ""}
        _url=((_d.get("meta") or {}).get("nextPageUrl")) or None
        _n+=1
    print("etapas de pipeline mapeadas:",len(ETAPA_DE),"| leads vivos en LEADS:",sum(1 for o in OPPS.values() if o["viva"]),flush=True)
except Exception as _e:
    print("AVISO: no se pudieron mapear etapas:",str(_e)[:70],flush=True)

# los leads vivos del pipeline entran en la descarga de fichas aunque no tengan cita
for _c,_o in OPPS.items():
    if _o["viva"] and _c not in cids: cids[_c]={"_solo_pipe":True}

def ev_name(info):
    # el titulo del evento del calendario trae "Nombre Lead - Reunion ..."; nombre fiable sin depender de la ficha
    for k in ("tri","clo"):
        t=((info.get(k) or {}).get("title") or "").strip()
        if not t: continue
        n=re.split(r'\s+-\s+',t)[0].strip()
        if n and not re.search(r'reuni|planificaci|validaci|introducci|estrateg|mensual|onboarding|asesor|mentor|entrevista',n,re.I):
            return n
    return ""
def fc(cid):
    for a in range(4):  # reintenta si viene vacia (429/throttle de GHL devuelve JSON sin 'contact')
        c=cg(f"https://services.leadconnectorhq.com/contacts/{cid}",headers=H21).get("contact",{})
        if c: return cid,c
        time.sleep(0.6*(a+1))
    return cid,{}
cmap={}
with ThreadPoolExecutor(max_workers=4) as ex:
    for cid,c in ex.map(fc,list(cids)): cmap[cid]=c

# setter por contactId, leido de la FICHA (campo "Setter asignada" o etiqueta): atribucion exacta,
# sin cruzar nombres. Cubre a todos los leads con cita; Kommo cubre el resto por nombre.
GSET={}
for _c3,_cc3 in cmap.items():
    _cm3={x.get("id"):x.get("value") for x in (_cc3 or {}).get("customFields",[])}
    _v3=str(_cm3.get("lcFBOFN6VjZhvTgMFvuf") or "").strip().capitalize()
    if _v3 not in ("Sary","Sara","Jesmary"):
        _tl3=[str(t).lower() for t in ((_cc3 or {}).get("tags") or [])]
        _v3=("Sary" if any("sary" in t for t in _tl3) else
             "Sara" if any(t=="sara" or "setter: sara" in t for t in _tl3) else
             "Jesmary" if any("jesmary" in t for t in _tl3) else "")
    if _v3: GSET[_c3]=_v3
print("setter por ficha (GSET):",len(GSET),flush=True)

# --- ULTIMO MENSAJE por contacto (24-ago-2026) ---
# Sirve para saber DE QUIEN ES EL TURNO en cada lead: si el ultimo mensaje es del lead, la pelota
# esta en nuestro tejado; si es nuestro, estamos esperando respuesta. Se saca del listado de
# conversaciones (una pasada paginada), sin bajar los mensajes uno a uno.
ULT={}; CONV_D0={}; CONV_D0N={}   # D0 por contactId y, de respaldo, por NOMBRE normalizado:
# el lead que agenda con el link roto crea una FICHA DUPLICADA, asi que su chat vive en otro contactId
# y solo el nombre permite unirlos.
try:
    _sa=None; _p=0
    while _p<140:
        _pr=[f"locationId={LOC}","limit=100","sortBy=last_message_date","sort=desc"]
        if _sa: _pr.append(f"startAfterDate={_sa}")
        _d=cg("https://services.leadconnectorhq.com/conversations/search",_pr)
        _cs=_d.get("conversations",[])
        if not _cs: break
        for _c in _cs:
            _cid=_c.get("contactId")
            if not _cid: continue
            _lmd=_c.get("lastMessageDate") or 0
            _da0r=_c.get("dateAdded")
            # el listado de conversaciones da dateAdded en EPOCH ms (no ISO como el calendario)
            if isinstance(_da0r,(int,float)) or (isinstance(_da0r,str) and _da0r.isdigit()):
                _ms=int(_da0r); _da0=dms(_ms if _ms>10**12 else _ms*1000) or ""
            else:
                _da0=str(_da0r or "")[:10]
            if _da0 and (_cid not in CONV_D0 or _da0<CONV_D0[_cid]): CONV_D0[_cid]=_da0
            _nm0=_knorm(_c.get("contactName") or _c.get("fullName") or "")
            if _da0 and _nm0 and len(_nm0)>4:
                for _k0 in {_nm0," ".join(_nm0.split()[:2])}:
                    if len(_k0)>4 and (_k0 not in CONV_D0N or _da0<CONV_D0N[_k0]): CONV_D0N[_k0]=_da0
            if _cid not in ULT or _lmd>ULT[_cid]["ts"]:
                ULT[_cid]={"ts":_lmd,"fecha":dms(_lmd) or "",
                           "dir":_c.get("lastMessageDirection") or "",
                           "txt":(_c.get("lastMessageBody") or "")[:220],
                           "tipo":_c.get("lastMessageType") or ""}
        if (_cs[-1].get("lastMessageDate") or 0) < cutoff: break
        _sa=_cs[-1].get("lastMessageDate"); _p+=1
    print("ultimo mensaje mapeado para",len(ULT),"contactos",flush=True)
except Exception as _e:
    print("AVISO: no se pudo mapear el ultimo mensaje:",str(_e)[:70],flush=True)
F={"prof":"I3MgyLftSnsPLPShebZH","setter":"lcFBOFN6VjZhvTgMFvuf","sf":"m7Sypf2v0DsMUl5EDv9D",
   "st":"BAdbcKq3A7Ks4kiaE9Vf","sc":"Gw71M4thYl2f0qTewdnV","rc":"dQQq7OBT7if2KbQv3mrx","cash":"fjnYS3QQDnOAhwa1je51",
   "ticket":"qSSpqvVhQqBMd01jwaiB","pagado":"Atuyg9PkXzUA0Na2OOxQ",
   "ss":"pmdl73DA4oYGPByvNdPE","nivel":"Jf5rP3LxylCANhTb4My9","obst":"JLP6SgW6EzLqwbP1rJjm",
   "presup":"Z7mdrH4OMIVxHoqSHMSC","ingresos":"R0ynaNT06KP0b8tggdZB","univ":"ihnDuK4eZxzSubfdRFcj",
   "urg":"fFb774tASOa5l3sjN6lV","comp":"cA3DiOdTpyG5dJi8hMb3","ig":"mA0HbCszoRU4syOjXAHQ",
   "canal":"eSHxDJMExlEXqP5tiBGn","closer":"X1bI7LUkc6wxJGuvMHrB","infocloser":"N4HJDy9VFhKhGCpwJoAk",
   "infolead":"FoBSAwhN7pZ9bRVk9h3o","objtri":"3adftx5fU0SS60Z9HfL7","objclo":"irbogxFInHAcRdPZuEPM",
   "estado":"3se8LQQqUMP1wp6CwXSZ","linktri":"EC5k5nHjjV9E5Vj6kkgp","linkclo":"EZqcLopGWnk2nUfMR5Yz",
   "motivo":"hTpq3AySxQLimEIlMKGp",   # Motivo principal (no cierre)
   "cta":"ycjnCtvt5bkIvlWzCO4v"}      # CTA Comentario (link del reel): de que reel/anuncio/cuenta vino el lead
def _num(v):
    try: return float(re.sub(r'[^0-9.]','',str(v))) if v not in (None,'') else 0.0
    except: return 0.0
def restri(tags):
    if "triage-cualifica" in tags: return "Cualifica"
    if "triage-no-cualifica" in tags: return "No cualifica"
    if "triage-seguimiento" in tags: return "Seguimiento"
    return ""
t_triCall=Counter(); t_triForm=Counter()
t_triLag=defaultdict(list)
leads=[]; closing=[]; triage_leads=[]
for cid,info in cids.items():
    c=cmap.get(cid) or {}
    cm={x.get("id"):x.get("value") for x in c.get("customFields",[])}
    tags=c.get("tags",[]) or []
    if info.get("tri"):
        _td=str(info["tri"].get("startTime"))[:10]
        if "triage-no-cualifica" in tags: t_nocual[_td]+=1
        if "triage-seguimiento" in tags: t_seg[_td]+=1
        if "triage-cualifica" in tags: t_cualT[_td]+=1
        # ASISTIÓ = grabación en Fathom (prueba directa) O resultado registrado por el formulario post-triaje
        _nm_ev=(c.get("contactName") or ev_name(info) or "")
        _grab=bool(_nm_ev) and _nk(_nm_ev) in FATHOM_TRI
        _res=any(t in tags for t in ("triage-cualifica","triage-no-cualifica","triage-seguimiento"))
        if _grab or _res: t_showT[_td]+=1
        if "triage-no-show" in tags: t_noshT[_td]+=1
        if "triage-no-confirma" in tags: t_nocfT[_td]+=1
        # cumplimiento del formulario post-triaje, imputado al dia de la llamada
        t_triCall[_td]+=1
        _em=(c.get("email") or "").lower().strip()
        _fp=post_by_cid.get(cid) or (post_by_mail.get(_em) if _em else None)
        if _fp:
            t_triForm[_td]+=1
            try:
                _d1=datetime.date(*map(int,_td.split("-"))); _d2=datetime.date(*map(int,_fp.split("-")))
                t_triLag[_td].append((_d2-_d1).days)
            except Exception: pass
    nombre=c.get("contactName") or ((c.get("firstName") or "")+" "+(c.get("lastName") or "")).strip() or ev_name(info) or "(sin nombre)"
    ficha=f"https://app.funnelup.io/v2/location/{LOC}/contacts/detail/{cid}"
    utm=((c.get("lastAttributionSource") or {}).get("utmSource") or (c.get("attributionSource") or {}).get("utmSource") or "").strip().lower()
    if utm in ("sara","sary","jesmary"):
        setter=utm.capitalize()  # utm_source del link de agenda = fuente principal y fiable
    else:
        _tl=[str(t).lower() for t in tags]
        setter=("Sara" if ("sara" in _tl or "setter: sara" in _tl) else
                ("Sary" if ("sary" in _tl or "setter: sary" in _tl) else
                 ("Jesmary" if ("jesmary" in _tl or "setter: jesmary" in _tl) else (cm.get(F["setter"]) or ""))))
    fagenda=str((info.get("tri") or {}).get("startTime") or (info.get("clo") or {}).get("startTime") or "")[:10]
    # solo leads REALES en la tabla (fichas fantasma/duplicadas sin nombre ni datos => fuera)
    es_real=(nombre and nombre!="(sin nombre)") or c.get("email") or c.get("phone") or cm.get(F["prof"])
    # Origen del lead: canal de afiliado (Antonie) vs funnel propio.
    # Se detecta por el utm_source del link de agenda o por la etiqueta, lo que llegue.
    origen = "antonie" if (utm == "antonie" or "antonie" in [str(t).lower() for t in tags]) else "propio"
    if es_real and not (info.get("_solo_pipe") and not info.get("tri") and not info.get("clo")):
      leads.append({"nombre":nombre,"setter":setter,"origen":origen,"prof":cm.get(F["prof"]) or "",
        "nivel":cm.get(F["nivel"]) or "","presup":cm.get(F["presup"]) or "","ingresos":cm.get(F["ingresos"]) or "",
        "univ":cm.get(F["univ"]) or "","urg":cm.get(F["urg"]) or "","comp":cm.get(F["comp"]) or "",
        "obst":cm.get(F["obst"]) or "","canal":cm.get(F["canal"]) or "","ig":cm.get(F["ig"]) or "",
        "email":c.get("email") or "","tf":c.get("phone") or "","fagenda":fagenda,
        "closer":cm.get(F["closer"]) or "","restri":restri(tags),"resclo":cm.get(F["rc"]) or "",
        "estado":cm.get(F["estado"]) or "","ss":cm.get(F["ss"]),"stri":cm.get(F["st"]),"sclo":cm.get(F["sc"]),
        "ticket":cm.get(F["ticket"]) or "","pagado":cm.get(F["pagado"]) or "",
        "objtri":cm.get(F["objtri"]) or "","objclo":cm.get(F["objclo"]) or "",
        "infocloser":cm.get(F["infocloser"]) or "","cta":cm.get(F["cta"]) or "","ficha":ficha})
    # UNA FILA POR CITA DE CLOSING (no una por lead): asi al filtrar una semana se ven todas las
    # llamadas que habia esa semana y que paso con cada una.
    _clist=info.get("clos") or ([info["clo"]] if "clo" in info else [])
    _nmL=(c.get("contactName") or ev_name(info) or nombre or "")
    _asis_clo=bool(_nmL) and _nk(_nmL) in FATHOM_CLO   # hay grabacion de su llamada de venta
    for e in _clist:
        # ¿se REAGENDO? = existe otra cita del mismo lead POSTERIOR a esta. Cero esfuerzo del
        # equipo: al reagendar se crea la cita nueva y esta pasa a "reagendada", no a "sin registrar".
        _post=[str(x.get("startTime"))[:10] for x in _clist if str(x.get("startTime"))>str(e.get("startTime"))]
        closing.append({"nombre":nombre,"fecha":str(e.get("startTime"))[:10],
            "hora":str(e.get("startTime"))[11:16],"estado":e.get("appointmentStatus",""),
            "reag":(min(_post) if _post else ""),
            "agendada":str(e.get("dateAdded"))[:10],
            "cta":cm.get(F["cta"]) or "","asistio":_asis_clo,
            "fuente":fuente_de(nombre,cm.get(F["cta"])),
            "resclo":cm.get(F["rc"]) or "","sc":cm.get(F["sc"]),"cash":cm.get(F["cash"]) or "",
            "ticket":cm.get(F["ticket"]) or "","pagado":cm.get(F["pagado"]) or "",
            # contexto para saber QUE PASO con cada uno sin salir del panel:
            "setter":setter,"closer":cm.get(F["closer"]) or "","prof":cm.get(F["prof"]) or "",
            "estadoclo":cm.get(F["estado"]) or "","motivo":cm.get(F["motivo"]) or "",
            "objclo":(cm.get(F["objclo"]) or "")[:400],"etapa":ETAPA_DE.get(cid,""),
            "tel":c.get("phone") or "","email":c.get("email") or "","ficha":ficha,
            # contexto del hilo: de quien es el turno ahora mismo
            "ult":(ULT.get(cid) or {}).get("fecha",""),"ultdir":(ULT.get(cid) or {}).get("dir",""),
            "ulttxt":(ULT.get(cid) or {}).get("txt",""),"ulttipo":(ULT.get(cid) or {}).get("tipo",""),
            # material para el resumen ampliado del pop-up
            "infoclo":(cm.get(F["infocloser"]) or "")[:1500],"restri":restri(tags),
            "ss":cm.get(F["ss"]),"stri":cm.get(F["st"]),
            "presup":cm.get(F["presup"]) or "","ingresos":cm.get(F["ingresos"]) or "",
            "nivel":cm.get(F["nivel"]) or "","urg":cm.get(F["urg"]) or ""})

    # ---- UNA FILA POR CITA DE TRIAJE (mismo formato que closing) ----
    _tlist=info.get("tris") or ([info["tri"]] if "tri" in info else [])
    _asis_tri=(bool(_nmL) and _nk(_nmL) in FATHOM_TRI) or restri(tags)!=""
    _fclo=min((str(x.get("startTime"))[:10] for x in _clist),default="")
    for e in _tlist:
        _post=[str(x.get("startTime"))[:10] for x in _tlist if str(x.get("startTime"))>str(e.get("startTime"))]
        triage_leads.append({"nombre":nombre,"fecha":str(e.get("startTime"))[:10],
            "hora":str(e.get("startTime"))[11:16],"estado":e.get("appointmentStatus",""),
            "reag":(min(_post) if _post else ""),
            "agendada":str(e.get("dateAdded"))[:10],
            "cta":cm.get(F["cta"]) or "","asistio":_asis_tri,"fclo":_fclo,
            "fuente":fuente_de(nombre,cm.get(F["cta"])),
            "d0":(CONV_D0.get(cid) or CONV_D0N.get(_knorm(nombre)) or CONV_D0N.get(" ".join(_knorm(nombre).split()[:2])) or ""),
            "restri":restri(tags),"stri":cm.get(F["st"]),"ss":cm.get(F["ss"]),
            "setter":setter,"triager":cm.get("HOpZ4zQsnwEs70pJSzea") or "","prof":cm.get(F["prof"]) or "",
            "motivo":cm.get("GWZs0fx5rdOsMiW8cvHM") or "","objtri":(cm.get(F["objtri"]) or "")[:400],
            "etapa":ETAPA_DE.get(cid,""),"tel":c.get("phone") or "","email":c.get("email") or "",
            "ficha":ficha,"presup":cm.get(F["presup"]) or "","ingresos":cm.get(F["ingresos"]) or "",
            "nivel":cm.get(F["nivel"]) or "","urg":cm.get(F["urg"]) or "",
            "infotri":(cm.get(F["infocloser"]) or "")[:1500],
            "restri_ia":(cm.get("tXb9dblrmzhtTZqdmBBj") or "")[:1200],
            "ult":(ULT.get(cid) or {}).get("fecha",""),"ultdir":(ULT.get(cid) or {}).get("dir",""),
            "ulttxt":(ULT.get(cid) or {}).get("txt",""),"ulttipo":(ULT.get(cid) or {}).get("tipo",""),
            # ¿tiene closing agendado despues del triaje? -> senal de que el triaje si avanzo
            "tiene_closing": bool(info.get("clos") or info.get("clo"))})
# ---- PESTANA PIPELINE: un lead por oportunidad VIVA del pipeline LEADS ----
pipeline_leads=[]
for _c,_o in OPPS.items():
    if not _o["viva"]: continue
    _cc=cmap.get(_c) or {}
    _cm={x.get("id"):x.get("value") for x in _cc.get("customFields",[])}
    _tg=_cc.get("tags",[]) or []
    _info=cids.get(_c,{})
    _nm=(_cc.get("contactName") or " ".join(x for x in [_cc.get("firstName"),_cc.get("lastName")] if x).strip()
         or _o["nombre"] or "(sin nombre)")
    # proxima cita futura (triaje o closing) si la hay
    _hoy=datetime.datetime.utcnow().strftime('%Y-%m-%d')
    _citas=sorted(str(x.get("startTime"))[:10] for x in (_info.get("tris") or [])+(_info.get("clos") or []) if x.get("startTime"))
    _prox=next((f for f in _citas if f>=_hoy),"")
    _u=ULT.get(_c) or {}
    pipeline_leads.append({"cid":_c,"nombre":_nm,"etapa":_o["etapa"],"desde":_o["desde"],
        "fuente":fuente_de(_nm,_cm.get(F["cta"])),
        "prox":_prox,"setter":_cm.get(F["setter"]) or "","cta":_cm.get(F["cta"]) or "",
        "prof":_cm.get(F["prof"]) or "","restri":restri(_tg),"resclo":_cm.get(F["rc"]) or "",
        "ticket":_cm.get(F["ticket"]) or "","pagado":_cm.get(F["pagado"]) or "",
        "motivo":_cm.get(F["motivo"]) or "","tel":_cc.get("phone") or _o["tel"],"email":_cc.get("email") or _o["email"],
        "presup":_cm.get(F["presup"]) or "","urg":_cm.get(F["urg"]) or "",
        "nivel":_cm.get(F["nivel"]) or "","ss":_cm.get(F["ss"]),"stri":_cm.get(F["st"]),"sc":_cm.get(F["sc"]),
        "ult":_u.get("fecha",""),"ultdir":_u.get("dir",""),"ulttxt":_u.get("txt",""),"ulttipo":_u.get("tipo",""),
        "ficha":f"https://app.funnelup.io/v2/location/{LOC}/contacts/detail/{_c}"})
print("pipeline_leads:",len(pipeline_leads),flush=True)

leads.sort(key=lambda r:(r["nombre"] or "").lower())
triage_leads.sort(key=lambda r:r["fecha"],reverse=True)
closing.sort(key=lambda r:r["fecha"],reverse=True)
# serie diaria de closing (alineada con days[])
cd={d:{"agendados":0,"showed":0,"noshow":0,"cancelled":0,"confirmed":0,"vendido":0,"facturacion":0.0,"cash":0.0} for d in days}
for r in closing:
    d=r["fecha"]
    # flag explicito de venta (mismo criterio que closing_daily) para la tabla de vendidos;
    # sobrevive a la version EQUIPO, que elimina ticket/pagado
    r["venta"]=1 if ((r.get("resclo")=="Vendido") or _num(r.get("ticket"))>0) else 0
    if d not in cd: continue
    cd[d]["agendados"]+=1
    if r["estado"] in cd[d] and r["estado"]!="showed": cd[d][r["estado"]]+=1
    # asistencia inferida como en triaje: grabacion de la llamada o resultado registrado
    if r["estado"]=="showed" or r.get("asistio") or (r.get("resclo") or "").strip(): cd[d]["showed"]+=1
    vend=(r.get("resclo")=="Vendido") or _num(r.get("ticket"))>0
    if vend: cd[d]["vendido"]+=1; cd[d]["facturacion"]+=_num(r.get("ticket"))
    cd[d]["cash"]+=_num(r.get("pagado")) or _num(r.get("cash"))
closing_daily=[dict(dia=d,**cd[d]) for d in days]
print("leads camino:",len(leads),"| closings:",len(closing),flush=True)

# 2) mensajes EN PARALELO
DM_TYPES=("TYPE_INSTAGRAM","TYPE_FACEBOOK")
def fetch(c):
    # Solo baja mensajes hasta RECFROM_TS (en incremental = ~12 días; en backfill = febrero).
    # 'reached' = True si se agotó la conversación (tenemos su PRIMER mensaje) -> se puede clasificar como "nueva".
    cid=c["id"]; out=[]; lastId=None; reached=True
    for _pg in range(25):
        params=["limit=100"]
        if lastId: params.append(f"lastMessageId={lastId}")
        m=cg(f"https://services.leadconnectorhq.com/conversations/{cid}/messages",params)
        mm=m.get("messages",{})
        msgs=mm.get("messages",[]) if isinstance(mm,dict) else (mm or [])
        if not msgs: break
        oldest=None
        for x in msgs:
            da=x.get("dateAdded")
            try: t=datetime.datetime.fromisoformat(da.replace("Z","+00:00")).timestamp() if da else None
            except: t=None
            if t is None: continue
            if oldest is None or t<oldest: oldest=t
            if x.get("messageType") in DM_TYPES:  # solo DMs de IG/FB cuentan como setting
                out.append({"dir":x.get("direction"),"body":x.get("body") or "","t":t})
        nextp=mm.get("nextPage") if isinstance(mm,dict) else False
        lastId=mm.get("lastMessageId") if isinstance(mm,dict) else None
        if not nextp or not lastId: break
        if oldest is not None and oldest*1000<RECFROM_TS: reached=False; break  # hay mensajes más viejos que la ventana
    out.sort(key=lambda x:x["t"]); return cid,out,reached
results={}
with ThreadPoolExecutor(max_workers=6) as ex:
    for i,(cid,ms,reached) in enumerate(ex.map(fetch,convs)):
        results[cid]=(ms,reached)
        if (i+1)%200==0: print("...msgs",i+1,flush=True)
print("mensajes descargados",flush=True)

# 3) KPIs setting por día
s_in=Counter(); s_out=Counter(); s_total=defaultdict(set); s_fu=Counter(); s_prop=Counter(); resp=defaultdict(list)
s_link=Counter(); s_badlink=Counter()  # propuestas con LINK de agenda real · de esas, las que van sin contact_id (=> duplicado)
resp_pairs=[]  # pares 1er inbound -> 1ª respuesta, con timestamps UTC, para filtrar horario activo en el panel
sh_horas=defaultdict(lambda: defaultdict(lambda: [0]*24))   # dia -> setter -> [24] mensajes salientes por hora UTC
for c in convs:
    ms,reached=results.get(c["id"],([],True))
    if not ms: continue
    _st=GSET.get(c.get("contactId")) or setter_de(c.get("contactName") or c.get("fullName") or "")
    fday=dms(int(ms[0]["t"]*1000))
    if reached and fday>=RECFROM:  # "nueva" solo si tenemos su PRIMER mensaje y cae en la ventana (evita recontar convs viejas)
        if ms[0]["dir"]=="inbound":
            s_in[fday]+=1; fin=ms[0]["t"]
            rep=next((x["t"] for x in ms if x["dir"]=="outbound" and x["t"]>=fin),None)
            if rep:
                el=(rep-fin)/60.0
                if el<=1440: resp[fday].append(el)
                if el<=4320: resp_pairs.append({"dia":fday,"in":int(fin),"rep":int(rep),"s":_st})  # hasta 3 días (la noche se descuenta luego)
        elif ms[0]["dir"]=="outbound": s_out[fday]+=1
    prev=None; proposed=False; linked=False
    _dprop=""   # primer dia en que se envio el link/propuesta en esta conversacion
    for x in ms:
        dd=dms(int(x["t"]*1000))
        if x["dir"]=="outbound":
            if dd>=RECFROM:
                sh_horas[dd][_st][datetime.datetime.utcfromtimestamp(x["t"]).hour]+=1
            if not _dprop and LINK.search(x["body"]): _dprop=dd
            if dd>=RECFROM and prev=="outbound": s_fu[dd]+=1
            if not proposed and LINK.search(x["body"]):
                if dd>=RECFROM: s_prop[dd]+=1
                proposed=True  # latch aunque sea día congelado, para no recontar la propuesta
            # propuesta con LINK real: la señal precisa. Y avisamos si el link va roto (=> ficha duplicada)
            if not linked and AGLINK.search(x["body"]):
                if dd>=RECFROM:
                    s_link[dd]+=1
                    if BADLINK.search(x["body"]): s_badlink[dd]+=1
                linked=True
        if dd>=RECFROM: s_total[dd].add(c["id"])
        prev=x["dir"]
    if reached:   # solo con la conversacion completa sabemos su primer mensaje real
        _old=_cache["convs"].get(c["id"],{})
        _cache["convs"][c["id"]]={"s":_st or _old.get("s",""),"d0":fday,
                                  "dp":(_dprop or _old.get("dp",""))}

# 5) MERGE en la caché con MÁXIMO: el estrangulamiento solo puede PERDER datos, nunca inventarlos.
# Quedándonos con el valor más alto, un run estrangulado NUNCA baja un día ya bueno (solo puede subirlo).
_rp=defaultdict(list)
for p in resp_pairs: _rp[p["dia"]].append(p)
for d in days:
    if d<RECFROM: continue
    new={"inb":s_in[d],"out":s_out[d],"total":len(s_total[d]),"fups":s_fu[d],"prop":s_prop[d],
         "link":s_link[d],"badlink":s_badlink[d]}
    old=_cache["days"].get(d,{})
    merged={k:max(int(new.get(k,0)),int(old.get(k) or 0)) for k in new}
    if new["total"]>=int(old.get("total") or 0):  # el run más completo manda en tiempo de respuesta
        merged["resp_min"]=(round(statistics.median(resp[d])) if resp[d] else old.get("resp_min"))
        _cache["resp_pairs"][d]=_rp.get(d,[])
    else:
        merged["resp_min"]=old.get("resp_min")
    _cache["days"][d]=merged
for _d2 in list(sh_horas):
    if _d2>=RECFROM: _cache["horas"][_d2]={k:v for k,v in sh_horas[_d2].items()}
json.dump(_cache,open(CACHE_PATH,"w"),ensure_ascii=False)
# serie de setting construida DESDE la caché (agendas se calcula fresco del calendario)
setting=[]
for d in days:
    c=_cache["days"].get(d,{})
    setting.append({"dia":d,"inb":c.get("inb",0),"out":c.get("out",0),
                    "nuevas":c.get("inb",0)+c.get("out",0),"total":c.get("total",0),
                    "fups":c.get("fups",0),"prop":c.get("prop",0),
                    "link":c.get("link",0),"badlink":c.get("badlink",0),
                    "agendas":sum(t_status[d].values()),"resp_min":c.get("resp_min")})
resp_pairs=[p for lst in _cache["resp_pairs"].values() for p in lst]
# FIX 25-jul: showed/noshow salen de las ETIQUETAS del formulario post-triaje (fuente real),
# con el appointmentStatus del calendario como respaldo (se toma el mayor: una fuente solo puede perder datos).
triage=[{"dia":d,"agendados":sum(t_status[d].values()),
         "showed":max(t_showT[d],t_status[d].get("showed",0)),
         "noshow":max(t_noshT[d],t_status[d].get("noshow",0)),
         "noconfirma":t_nocfT[d],
         "cancelled":t_status[d].get("cancelled",0),
         "confirmed":t_status[d].get("confirmed",0),
         "cualifica":max(t_cualT[d],t_cual[d]),"nocualifica":t_nocual[d],"seguimiento":t_seg[d]} for d in days]
# detectar tramos SIN DATOS de setting (huecos interiores de sincronización GHL↔Instagram)
gaps=[]; i=0; N=len(setting)
while i<N:
    if setting[i]["total"]==0:
        j=i
        while j+1<N and setting[j+1]["total"]==0: j+=1
        antes=any(setting[k]["total"]>0 for k in range(0,i))
        despues=any(setting[k]["total"]>0 for k in range(j+1,N))
        if antes and despues and (j-i+1)>=2:  # solo huecos interiores de 2+ días
            gaps.append({"from":setting[i]["dia"],"to":setting[j]["dia"]})
        i=j+1
    else: i+=1
for r in setting:
    r["nodata"]=any(g["from"]<=r["dia"]<=g["to"] for g in gaps)
print("tramos sin datos setting:",gaps,flush=True)
# GUARDIÁN anti-run-degradado: leads/triaje/closing NO tienen caché (se bajan del calendario+fichas cada vez).
# Si un run se estrangula y el calendario devuelve vacío (0 leads Y 0 agendas), NO publiques vacío:
# conserva leads/triaje/closing del data.json anterior (el setting sí es fresco por su caché).
try:
    prev=json.load(open(os.path.join(OUTDIR,"data.json")))
except Exception as e:
    prev=None; print("guardián: sin data.json previo",e,flush=True)
if prev:
    # 24-ago-2026: el guardián se hace POR SECCIÓN. Antes solo saltaba si TODO venía vacío, así que
    # un run donde el calendario de triaje respondía pero el de closing se estrangulaba publicaba
    # closing=[] y borraba la pestaña entera (pasó el 24-ago 13:11).
    if len(leads)==0 and prev.get("leads"):
        print("AVISO: 0 leads -> conservo los anteriores",flush=True); leads=prev["leads"]
    if sum(x["agendados"] for x in triage)==0 and sum(x.get("agendados",0) for x in prev.get("triage",[])):
        print("AVISO: 0 agendas de triaje -> conservo las anteriores",flush=True); triage=prev["triage"]
    if len(closing)==0 and prev.get("closing"):
        print("AVISO: 0 closings (calendario estrangulado) -> conservo los anteriores",flush=True)
        closing=prev["closing"]; closing_daily=prev.get("closing_daily",closing_daily)
    if len(triage_leads)==0 and prev.get("triage_leads"):
        print("AVISO: 0 llamadas de triaje -> conservo las anteriores",flush=True)
        triage_leads=prev["triage_leads"]
SETTERS_ACT=["Sary","Sara","Jesmary"]
# ---- OBJETIVOS DIARIOS (pestana "Dia"): estandar de calidad del funnel, fijado con Jorge 26-ago.
# Tasas fijadas por Jorge (26-ago; base bajada a 150 = 50/setter sin inbound, decidido 26-ago tarde): 3% conv->prop (subir a 4% el 1-oct si mejora el inbound) ->
# 60% prop->agenda -> 75% asistencia triaje -> 67% cualifica -> 85% asistencia closing -> 50% cierre.
# La palanca si falta resultado son las CONVERSACIONES (la fila de arriba lleva objetivo en numeros
# absolutos, no en %). Cadena resultante: 170 -> 5 -> 3 -> 2,25 -> 1,5 -> 1,275
# -> 1,6 ASISTEN al closing (80%: pedido por Jorge, el 100% anterior era ingenuo)
# -> 0,8 ventas/dia (cierre del 50%: estandar de Jorge; el historico real es 16%, el chip ira
#    rojo hasta que el cierre mejore, y esa es la funcion). EDITABLES en targets.json.
# 26-ago (tarde): Jorge sube el minimo a 100 conversaciones POR SETTER (x3 = 300/dia) y la
# cadena se reescala manteniendo sus tasas. Pendiente: decidir que tasas exigentes se relajan.
TARGETS={"conversaciones": 150, "propuestas": 4.5, "agendas": 2.7, "triajes_hechos": 2.03, "cualifica": 1.36, "closings": 1.15, "ventas": 0.58, "facturacion": 1700, "cash": 1300}
try: TARGETS.update({k:v for k,v in json.load(open(os.path.join(HERE,"targets.json"))).items() if k in TARGETS})
except Exception: pass
_kpi_dias=defaultdict(set)
for _k in kpis: _kpi_dias[_k["dia"]].add(_k["setter"])
cumplimiento=[]
for d in days:
    _n=t_triCall.get(d,0); _f=t_triForm.get(d,0); _lags=t_triLag.get(d,[])
    cumplimiento.append({"dia":d,"triages":_n,"con_form":_f,"sin_form":max(0,_n-_f),
        "estado_triage":("No hubo llamadas" if _n==0 else ("Completo" if _f>=_n else ("Parcial" if _f>0 else "Sin registrar"))),
        "retraso_medio":(round(sum(_lags)/len(_lags),1) if _lags else None),
        "kpis":{s:(s in _kpi_dias.get(d,set())) for s in SETTERS_ACT}})
data={"generado":datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),"rango":f"{days[0]} a {days[-1]}","setting":setting,"triage":triage,"leads":leads,"closing":closing,"closing_daily":closing_daily,"gaps":gaps,"resp_pairs":resp_pairs,"kpis":kpis,"cumplimiento":cumplimiento,"setters":SETTERS_ACT,"triage_leads":triage_leads,"targets":TARGETS,"pipeline_leads":pipeline_leads,"etapas_orden":ETAPAS_ORDEN,"horas":[{"dia":d,"s":st,"h":h} for d,m in _cache["horas"].items() for st,h in m.items()],"convprop":[{"s":v.get("s",""),"d0":v.get("d0",""),"dp":v.get("dp","")} for v in _cache["convs"].values()]}
json.dump(data,open(os.path.join(OUTDIR,"data.json"),"w"),ensure_ascii=False,indent=1)
tpl=open(os.path.join(HERE,"template.html")).read()
html=tpl.replace("/*DATA*/","const DATA = "+json.dumps(data,ensure_ascii=False)+";")
open(os.path.join(OUTDIR,"dashboard.html"),"w").write(html)
# versión EQUIPO: misma info SIN dinero (se ELIMINA del embed, no solo se oculta)
import copy
td=copy.deepcopy(data)
for l in td["leads"]: l["ticket"]=""; l["pagado"]=""
for r in td["closing"]: r["cash"]=""; r["ticket"]=""; r["pagado"]=""
for r in td["closing_daily"]: r["facturacion"]=0; r["cash"]=0
td["targets"]={k:v for k,v in TARGETS.items() if k not in ("facturacion","cash")}
for r in td.get("pipeline_leads",[]): r["ticket"]=""; r["pagado"]=""
thtml=tpl.replace("/*DATA*/","window.TEAM=true; const DATA = "+json.dumps(td,ensure_ascii=False)+";")
open(os.path.join(OUTDIR,"equipo.html"),"w").write(thtml)
print("OK dashboard.html + equipo.html generados",flush=True)
# publicar en GitHub Pages (en local). En CI (SKIP_DEPLOY=1) lo publica el propio workflow.
if not os.environ.get("SKIP_DEPLOY"):
    subprocess.run(["python3",os.path.join(HERE,"deploy_github.py")])

