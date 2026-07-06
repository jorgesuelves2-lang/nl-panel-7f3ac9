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
TRIAGE_CAL="2EY5mRYqpaAx4qfnsWJM"; START="2026-02-01"  # fecha más antigua que se descarga
HERE=os.path.dirname(os.path.abspath(__file__))
OUTDIR=os.environ.get("OUTDIR","/Users/jorgesuelves/Desktop/Claude Code/marcas/natscholibre/dashboard")
os.makedirs(OUTDIR,exist_ok=True)
def cg(url,params=[],headers=H):
    for a in range(5):
        r=subprocess.run(["curl","-s","-m","25","-G",url,*sum([["--data-urlencode",p] for p in params],[]),*headers],capture_output=True,text=True).stdout
        if r:
            try: return json.loads(r)
            except: pass
        time.sleep(0.4*(a+1))
    return {}
def dms(ms): return datetime.datetime.utcfromtimestamp(ms/1000).strftime('%Y-%m-%d') if ms else None
now=int(time.time()*1000)
_sdt=datetime.datetime.strptime(START,'%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
cutoff=int(_sdt.timestamp()*1000)
DAYS=(datetime.datetime.now(datetime.timezone.utc).date()-_sdt.date()).days+1
days=[(_sdt+datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(DAYS)]
LINK=re.compile(r'agendaor|agendaror|natscholibre\.com/agenda|ag[eé]ndame|agendar|calendario|te paso el (link|calendario)',re.I)
# --- CACHÉ de setting: los días pasados se CONGELAN (no se re-descargan 8000 conversaciones cada vez).
# Solo se recalculan los últimos RECOMPUTE_DAYS días. La primera vez (sin caché) = BACKFILL completo. ---
CACHE_PATH=os.path.join(OUTDIR,"setting_cache.json")
try: _cache=json.load(open(CACHE_PATH))
except Exception: _cache={}
_cache.setdefault("days",{}); _cache.setdefault("resp_pairs",{})
RECOMPUTE_DAYS=int(os.environ.get("RECOMPUTE_DAYS","12"))
BACKFILL=(not _cache["days"]) or os.environ.get("BACKFILL")=="1"
RECFROM=START if BACKFILL else (datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=RECOMPUTE_DAYS)).strftime('%Y-%m-%d')
RECFROM_TS=int(datetime.datetime.strptime(RECFROM,'%Y-%m-%d').replace(tzinfo=datetime.timezone.utc).timestamp()*1000)
print(("setting: BACKFILL completo" if BACKFILL else f"setting: incremental (recalcula desde {RECFROM})"),flush=True)

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

# 1b) triaje + closing (calendario) PRIMERO, para que no le afecte el rate-limit de los mensajes
ev=cg(f"https://services.leadconnectorhq.com/calendars/events?locationId={LOC}&calendarId={TRIAGE_CAL}&startTime={cutoff}&endTime={now}",headers=H21).get("events",[])
t_status=defaultdict(Counter)
for e in ev: t_status[str(e.get("startTime"))[:10]][e.get("appointmentStatus","?")]+=1
# contactos que pasaron a CLOSING (calendarios de Planificación Estratégica)
endf=now+30*86400*1000; closing_contacts=set()
for cal in ["VRaGr4KGSZNiuDamyV4q","ODbNZytVDUxJxry4QzmX"]:
    for e in cg(f"https://services.leadconnectorhq.com/calendars/events?locationId={LOC}&calendarId={cal}&startTime={cutoff}&endTime={endf}",headers=H21).get("events",[]):
        if e.get("contactId"): closing_contacts.add(e["contactId"])
t_cual=defaultdict(int)
for e in ev:
    if e.get("contactId") and e["contactId"] in closing_contacts: t_cual[str(e.get("startTime"))[:10]]+=1
t_nocual=defaultdict(int)  # se rellena en el bucle de leads (depende de las etiquetas de la ficha)
print("eventos triaje:",len(ev),"| contactos con closing:",len(closing_contacts),flush=True)

# 1c) CAMINO DE LOS LEADS + CLOSING (desde START) — antes de los mensajes para evitar rate-limit
LJcut=cutoff
def evs(cal,s,e): return cg(f"https://services.leadconnectorhq.com/calendars/events?locationId={LOC}&calendarId={cal}&startTime={s}&endTime={e}",headers=H21).get("events",[])
cids={}
for e in evs(TRIAGE_CAL,LJcut,now):
    if e.get("contactId"): cids.setdefault(e["contactId"],{})["tri"]=e
for cal in ["VRaGr4KGSZNiuDamyV4q","ODbNZytVDUxJxry4QzmX"]:
    for e in evs(cal,LJcut,endf):
        if e.get("contactId"): cids.setdefault(e["contactId"],{})["clo"]=e
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
F={"prof":"I3MgyLftSnsPLPShebZH","setter":"lcFBOFN6VjZhvTgMFvuf","sf":"m7Sypf2v0DsMUl5EDv9D",
   "st":"BAdbcKq3A7Ks4kiaE9Vf","sc":"Gw71M4thYl2f0qTewdnV","rc":"dQQq7OBT7if2KbQv3mrx","cash":"fjnYS3QQDnOAhwa1je51",
   "ticket":"qSSpqvVhQqBMd01jwaiB","pagado":"Atuyg9PkXzUA0Na2OOxQ",
   "ss":"pmdl73DA4oYGPByvNdPE","nivel":"Jf5rP3LxylCANhTb4My9","obst":"JLP6SgW6EzLqwbP1rJjm",
   "presup":"Z7mdrH4OMIVxHoqSHMSC","ingresos":"R0ynaNT06KP0b8tggdZB","univ":"ihnDuK4eZxzSubfdRFcj",
   "urg":"fFb774tASOa5l3sjN6lV","comp":"cA3DiOdTpyG5dJi8hMb3","ig":"mA0HbCszoRU4syOjXAHQ",
   "canal":"eSHxDJMExlEXqP5tiBGn","closer":"X1bI7LUkc6wxJGuvMHrB","infocloser":"N4HJDy9VFhKhGCpwJoAk",
   "infolead":"FoBSAwhN7pZ9bRVk9h3o","objtri":"3adftx5fU0SS60Z9HfL7","objclo":"irbogxFInHAcRdPZuEPM",
   "estado":"3se8LQQqUMP1wp6CwXSZ","linktri":"EC5k5nHjjV9E5Vj6kkgp","linkclo":"EZqcLopGWnk2nUfMR5Yz"}
def _num(v):
    try: return float(re.sub(r'[^0-9.]','',str(v))) if v not in (None,'') else 0.0
    except: return 0.0
def restri(tags):
    if "triage-cualifica" in tags: return "Cualifica"
    if "triage-no-cualifica" in tags: return "No cualifica"
    if "triage-seguimiento" in tags: return "Seguimiento"
    return ""
leads=[]; closing=[]
for cid,info in cids.items():
    c=cmap.get(cid) or {}
    cm={x.get("id"):x.get("value") for x in c.get("customFields",[])}
    tags=c.get("tags",[]) or []
    if info.get("tri") and "triage-no-cualifica" in tags:
        t_nocual[str(info["tri"].get("startTime"))[:10]]+=1
    nombre=c.get("contactName") or ((c.get("firstName") or "")+" "+(c.get("lastName") or "")).strip() or ev_name(info) or "(sin nombre)"
    ficha=f"https://app.funnelup.io/v2/location/{LOC}/contacts/detail/{cid}"
    utm=((c.get("lastAttributionSource") or {}).get("utmSource") or (c.get("attributionSource") or {}).get("utmSource") or "").strip().lower()
    if utm in ("sara","sary"):
        setter=utm.capitalize()  # utm_source del link de agenda = fuente principal y fiable
    else:
        setter="Sara" if "Sara" in tags else ("Sary" if "Sary" in tags else (cm.get(F["setter"]) or ""))
    fagenda=str((info.get("tri") or {}).get("startTime") or (info.get("clo") or {}).get("startTime") or "")[:10]
    # solo leads REALES en la tabla (fichas fantasma/duplicadas sin nombre ni datos => fuera)
    es_real=(nombre and nombre!="(sin nombre)") or c.get("email") or c.get("phone") or cm.get(F["prof"])
    if es_real:
      leads.append({"nombre":nombre,"setter":setter,"prof":cm.get(F["prof"]) or "",
        "nivel":cm.get(F["nivel"]) or "","presup":cm.get(F["presup"]) or "","ingresos":cm.get(F["ingresos"]) or "",
        "univ":cm.get(F["univ"]) or "","urg":cm.get(F["urg"]) or "","comp":cm.get(F["comp"]) or "",
        "obst":cm.get(F["obst"]) or "","canal":cm.get(F["canal"]) or "","ig":cm.get(F["ig"]) or "",
        "email":c.get("email") or "","tf":c.get("phone") or "","fagenda":fagenda,
        "closer":cm.get(F["closer"]) or "","restri":restri(tags),"resclo":cm.get(F["rc"]) or "",
        "estado":cm.get(F["estado"]) or "","ss":cm.get(F["ss"]),"stri":cm.get(F["st"]),"sclo":cm.get(F["sc"]),
        "ticket":cm.get(F["ticket"]) or "","pagado":cm.get(F["pagado"]) or "",
        "objtri":cm.get(F["objtri"]) or "","objclo":cm.get(F["objclo"]) or "",
        "infocloser":cm.get(F["infocloser"]) or "","ficha":ficha})
    if "clo" in info:
        e=info["clo"]
        closing.append({"nombre":nombre,"fecha":str(e.get("startTime"))[:10],"estado":e.get("appointmentStatus",""),
            "resclo":cm.get(F["rc"]) or "","sc":cm.get(F["sc"]),"cash":cm.get(F["cash"]) or "",
            "ticket":cm.get(F["ticket"]) or "","pagado":cm.get(F["pagado"]) or "","ficha":ficha})
leads.sort(key=lambda r:(r["nombre"] or "").lower())
closing.sort(key=lambda r:r["fecha"],reverse=True)
# serie diaria de closing (alineada con days[])
cd={d:{"agendados":0,"showed":0,"noshow":0,"cancelled":0,"confirmed":0,"vendido":0,"facturacion":0.0,"cash":0.0} for d in days}
for r in closing:
    d=r["fecha"]
    if d not in cd: continue
    cd[d]["agendados"]+=1
    if r["estado"] in cd[d]: cd[d][r["estado"]]+=1
    vend=(r.get("resclo")=="Vendido") or _num(r.get("ticket"))>0
    if vend: cd[d]["vendido"]+=1; cd[d]["facturacion"]+=_num(r.get("ticket"))
    cd[d]["cash"]+=_num(r.get("pagado")) or _num(r.get("cash"))
closing_daily=[dict(dia=d,**cd[d]) for d in days]
print("leads camino:",len(leads),"| closings:",len(closing),flush=True)

# 2) mensajes EN PARALELO
DM_TYPES=("TYPE_INSTAGRAM","TYPE_FACEBOOK")
def fetch(c):
    cid=c["id"]; out=[]; lastId=None
    for _pg in range(25):  # pagina el historial completo (hasta 25 páginas = 2500 msgs/conv)
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
        if oldest is not None and oldest*1000<cutoff: break  # ya pasamos febrero
    out.sort(key=lambda x:x["t"]); return cid,out
results={}
with ThreadPoolExecutor(max_workers=6) as ex:
    for i,(cid,ms) in enumerate(ex.map(fetch,convs)):
        results[cid]=ms
        if (i+1)%200==0: print("...msgs",i+1,flush=True)
print("mensajes descargados",flush=True)

# 3) KPIs setting por día
s_in=Counter(); s_out=Counter(); s_total=defaultdict(set); s_fu=Counter(); s_prop=Counter(); resp=defaultdict(list)
resp_pairs=[]  # pares 1er inbound -> 1ª respuesta, con timestamps UTC, para filtrar horario activo en el panel
for c in convs:
    ms=results.get(c["id"],[])
    if not ms: continue
    fday=dms(int(ms[0]["t"]*1000))
    if fday>=RECFROM:  # "nueva" solo si su PRIMER mensaje cae en la ventana recalculada (evita recontar convs viejas)
        if ms[0]["dir"]=="inbound":
            s_in[fday]+=1; fin=ms[0]["t"]
            rep=next((x["t"] for x in ms if x["dir"]=="outbound" and x["t"]>=fin),None)
            if rep:
                el=(rep-fin)/60.0
                if el<=1440: resp[fday].append(el)
                if el<=4320: resp_pairs.append({"dia":fday,"in":int(fin),"rep":int(rep)})  # hasta 3 días (la noche se descuenta luego)
        elif ms[0]["dir"]=="outbound": s_out[fday]+=1
    prev=None; proposed=False
    for x in ms:
        dd=dms(int(x["t"]*1000))
        if x["dir"]=="outbound":
            if dd>=RECFROM and prev=="outbound": s_fu[dd]+=1
            if not proposed and LINK.search(x["body"]):
                if dd>=RECFROM: s_prop[dd]+=1
                proposed=True  # latch aunque sea día congelado, para no recontar la propuesta
        if dd>=RECFROM: s_total[dd].add(c["id"])
        prev=x["dir"]

# 5) MERGE en la caché: los días recalculados se sobrescriben; los congelados se conservan
for d in days:
    if d<RECFROM: continue
    _cache["days"][d]={"inb":s_in[d],"out":s_out[d],"total":len(s_total[d]),"fups":s_fu[d],"prop":s_prop[d],
                       "resp_min":(round(statistics.median(resp[d])) if resp[d] else None)}
for d in [k for k in _cache["resp_pairs"] if k>=RECFROM]: del _cache["resp_pairs"][d]  # limpia recientes, se re-añaden
_rp=defaultdict(list)
for p in resp_pairs: _rp[p["dia"]].append(p)
for d,lst in _rp.items(): _cache["resp_pairs"][d]=lst
json.dump(_cache,open(CACHE_PATH,"w"),ensure_ascii=False)
# serie de setting construida DESDE la caché (agendas se calcula fresco del calendario)
setting=[]
for d in days:
    c=_cache["days"].get(d,{})
    setting.append({"dia":d,"inb":c.get("inb",0),"out":c.get("out",0),
                    "nuevas":c.get("inb",0)+c.get("out",0),"total":c.get("total",0),
                    "fups":c.get("fups",0),"prop":c.get("prop",0),
                    "agendas":sum(t_status[d].values()),"resp_min":c.get("resp_min")})
resp_pairs=[p for lst in _cache["resp_pairs"].values() for p in lst]
triage=[{"dia":d,"agendados":sum(t_status[d].values()),"showed":t_status[d].get("showed",0),
         "noshow":t_status[d].get("noshow",0),"cancelled":t_status[d].get("cancelled",0),
         "confirmed":t_status[d].get("confirmed",0),"cualifica":t_cual[d],"nocualifica":t_nocual[d]} for d in days]
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
data={"generado":datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),"rango":f"{days[0]} a {days[-1]}","setting":setting,"triage":triage,"leads":leads,"closing":closing,"closing_daily":closing_daily,"gaps":gaps,"resp_pairs":resp_pairs}
json.dump(data,open(os.path.join(OUTDIR,"data.json"),"w"),ensure_ascii=False,indent=1)
html=open(os.path.join(HERE,"template.html")).read().replace("/*DATA*/","const DATA = "+json.dumps(data,ensure_ascii=False)+";")
open(os.path.join(OUTDIR,"dashboard.html"),"w").write(html)
print("OK dashboard.html generado",flush=True)
# publicar en GitHub Pages (en local). En CI (SKIP_DEPLOY=1) lo publica el propio workflow.
if not os.environ.get("SKIP_DEPLOY"):
    subprocess.run(["python3",os.path.join(HERE,"deploy_github.py")])

