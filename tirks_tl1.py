import pandas as pd
from collections import Counter, namedtuple, OrderedDict
import csv
import re
from itertools import chain
from typing import Dict, List, Tuple, Any

data = '/Users/david/PycharmProjects/TIRKS/tirks_data/'
source = '/Users/david/PycharmProjects/TIRKS/'
VERBOSE = False
TID = 'BHLHPABEK31'
DCS_CIRCUITS = source + 'DCS_Circuits.xlsx'
ST01_CHILDREN = source + 'ST01_Children.xlsx'
T3Z_CHILDREN = source + 'T3Z_Children.xlsx'
lo_exceptions = []
tl1_sources = [source + '5500 TL1 BHLHPABEK31 180710.xlsx', source + 'june26.xlsx']


def print_exception(_resp, _response):
    global lo_exceptions
    #     print(len(_resp), _resp)
    #     print(_response)
    lo_exceptions.append(_resp)


def scrub_response(response) -> Tuple[List, str]:
    # fix the ####-## DGR issues - give the DGR key an empty value...
    if re.match(r'^[0-9]{4}-[0-9]{2}:DGR', response):
        response = response.replace(':DGR:', ':TYPE=DGR:')
        # print(response)
    # some responses have quotes... remove
    if response[0] == '"' == response[-1]:
        response = response[1:-2]
    response = re.sub('([\r\n\s+])', '', response)
    if response.startswith('"') and 'ERCDE' not in response:
        response = re.sub('\"+', '"', response)
        response = response[1:-1]
    resp = re.sub(":+", ":", response)
    resp = re.sub(",+", ",", resp)
    resp = [x for x in chain(*[r.split(',') for r in resp.split(':')])]
    resp = [x.split('=') for x in resp]
    return resp, response


def process_dcs() -> Tuple[Dict, Dict, Dict, List, List, List]:
    """

    :return:
    """
    dcs_df = pd.read_excel(DCS_CIRCUITS, converters={'DATE': pd.to_datetime,
                                                     'PRODUCT': str,
                                                     'ADDR1': str,
                                                     'ADDR4': str})
    dcs_df = dcs_df[dcs_df.STATUS != 'X']
    dcs_df = dcs_df[dcs_df.STATUS != 'H']
    dcs_df = dcs_df[dcs_df.SLOT.notnull()]
    dcs_df.dropna(subset=['ASSIGNMENT', 'Child Circuit'], how='all', inplace=True)
    dcs_df = dcs_df.fillna('')
    dcs_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
    dcs_df.sort_values('SLOT')
    # create and cleanup the ST1 & TZ3 dataframes
    st1_df = pd.read_excel(ST01_CHILDREN)
    st1_df = st1_df.fillna('')
    t3z_df = pd.read_excel(T3Z_CHILDREN)
    t3z_df = t3z_df.fillna('')
    # convert the dataframes to lists along with their headers
    lo_dcs = dcs_df.values.tolist()
    dcs_header: List = dcs_df.columns.values.tolist()
    lo_st1 = st1_df.values.tolist()
    st1_header: List = st1_df.columns.values.tolist()
    lo_t3z = t3z_df.values.tolist()
    t3z_header: List = t3z_df.columns.values.tolist()
    if VERBOSE:
        print("DCS TP", Counter(dcs_df.TP))
        print("DCS STATUS", Counter(dcs_df.STATUS))
        print("DCS SERVICES", Counter(dcs_df.SERVICES))
        print("DCS FR", Counter(dcs_df.FR))
        # print("A CLLI")
        # pprint(Counter(dcs_df.A_CLLI))
        # print("Z CLLI")
        # pprint(Counter(dcs_df.Z_CLLI))
        print("ST1 TP", Counter(st1_df.TP))
        print("T3Z TP", Counter(t3z_df.TP))
        # print("T3Z ASS", Counter(t3z_df.CKT_ID))
    # Create new DCS, TS1 & T3Z list items by splitting the ASSIGNMENT and the Child Circuit at the 1st "/"
    for i, x in enumerate(lo_dcs):
        temp = []
        if '/' in x[8]:
            temp.extend([z for z in x[8].split('/', 1)])
        else:
            temp.extend(['', ''])
        if '/' in x[19]:
            temp.extend([z for z in x[19].split('/', 1)])
        else:
            temp.extend(['', ''])
        if 'See' in x[19]:
            x[19] = ''
            if len(temp) == 2:
                temp.extend(['', ''])
        lo_dcs[i] = x + temp
    dcs_header.extend(['ASS_IDX', 'ASS_VAL', 'CHILD_IDX', 'CHILD_VAL'])
    for i, x in enumerate(lo_st1):
        if x[4]:
            lo_st1[i] = lo_st1[i] + [x for x in x[4].split('/', 1)]
        else:
            lo_st1[i] = lo_st1[i] + ['', '']
    st1_header.extend(['C_IDX', 'C_VAL'])
    for i, x in enumerate(lo_t3z):
        if x[4]:
            lo_t3z[i] = lo_t3z[i] + [x for x in x[4].split('/', 1)]
        else:
            lo_t3z[i] = lo_t3z[i] + ['', '']
    t3z_header.extend(['C_IDX', 'C_VAL'])
    # Create filed names for the namedtuples
    dcs_names: List[Any] = [re.sub(r'[ :/#]*', '', x.lower()) for x in dcs_header]
    st1_names: List[Any] = [re.sub(r'[ :/#]*', '', x.lower()) for x in st1_header]
    t3z_names: List[Any] = [re.sub(r'[ :/#]*', '', x.lower()) for x in t3z_header]
    # define the named tuples
    DCS = namedtuple('DCS', dcs_names)
    ST1 = namedtuple('ST1', st1_names)
    T3Z = namedtuple('T3Z', t3z_names)
    # populate the dictionaries of namedtuples
    dcs = OrderedDict()
    for dd in lo_dcs:
        dcs[dd[3] + dd[4]] = DCS(*dd)
    st1 = {}
    for s in lo_st1:
        st1[s[1]] = ST1(*s)
    t3z = {}
    for t in lo_t3z:
        t3z[t[1]] = T3Z(*t)

    if VERBOSE:
        # print the named tuples structures
        idcs = 4  # 30
        ist1 = 256
        it3z = 188
        print(f'\n{"DCS:"} [{idcs:>4}]')
        for idx, x in enumerate(lo_dcs[idcs]):
            print(f'{dcs_header[idx]:>15} {dcs_names[idx]:>15} [{idx:>4}] {x}')
        print(f'\n{"ST01:"} [{ist1:>4}]')
        for idx, x in enumerate(lo_st1[ist1]):
            print(f'{st1_header[idx]:>15} {st1_names[idx]:>15} [{idx:>4}] {x}')
        print(f'\n{"T3Z:"} [{it3z:>4}]')
        for idx, x in enumerate(lo_t3z[it3z]):
            print(f'{t3z_header[idx]:>15} {t3z_header[idx]:>15} [{idx:>4}] {x}')
    return dcs, st1, t3z, dcs_header, st1_header, t3z_header


def process_tl1():
    """

    :return:
    """
    global tl1_sources
    # tl1_sources = ['5500 TL1 BHLHPABEK31 180710.xlsx', 'june26.xlsx']
    tl1_file = tl1_sources[0]
    tl1_df = pd.read_excel(tl1_file)
    lo_tl1 = tl1_df.values.tolist()
    lo_dupes = []
    lo_skipped = []
    # Build namedtuples for each reponse type
    # PGC
    # PGC6
    resp = 'PGC1-0045-B:TYPE=11:HWV=81.5512A RD1,SN=IL2490467,DT=980310:PST=IS-NR-ACT'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PGC6 = namedtuple('PGC6', [x[0] for x in resp[1:]])
    # PGC5
    resp = 'PGC3-0417:TYPE=31:ALMPF=1,CONFIG=T1:PST=AS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PGC5 = namedtuple('PGC5', [x[0] for x in resp[1:]])
    # PCG4
    resp = 'PGC1-0001:TYPE=11:ALMPF=1:PST=AS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PGC4 = namedtuple('PGC4', [x[0] for x in resp[1:]])
    # PCG3
    resp = 'PGC1-0097:PRMTR=AID&TYPE,TYPE=31'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PGC3 = namedtuple('PGC3', [x[0] for x in resp[1:]])
    # PM9
    resp = "PM1-0004-17:TYPE=11A:HWV=81.5514ARD,SN=HL1966644,DT=931213,ALMPF=1,PRTN=ALW,PMPI=PASSIVE:PST=IS-NR-ACT"
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PM9 = namedtuple('PM9', [x[0] for x in resp[1:]])
    # PM4
    resp = 'PME-0977:ERCDE=SSNV,AID=PGCE-0977,PST=UAS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PM4 = namedtuple('PM4', [x[0] for x in resp[1:]])
    # PM2
    resp = "PME-0194:::PST=OOS-ANR-UEQ-UAS"
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PM2 = namedtuple('PM2', [x[0] for x in resp[1:]])
    # PP4
    resp = "PPS-0001-A::HWV=8X.5535,ALMPF=1:PST=NR"
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    PP4 = namedtuple('PP4', [x[0] for x in resp[1:]])
    # RPM
    # RPM10
    resp = 'RPM3-0497:TYPE=31A:HWV=81.5517A RO,SN=TZ0940672,DT=050803,ALMPF=1,PRTN=ALW,PRTN_PORT=PM3- 0502,PRTN_TYPE=AUTO:PST=IS-NR-ACT'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RPM10 = namedtuple('RPM10', [x[0] for x in resp[1:]])
    # RPM8
    resp = 'RPM1-0001:TYPE=11A:HWV=81.5514A RD,SN=HL1966827,DT=931209,ALMPF=1,PRTN=ALW:PST=ISNR-STBY'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RPM8 = namedtuple('RPM8', [x[0] for x in resp[1:]])
    # RPM2
    resp = 'RPM1D-0395:::PST=OOS-ANR-UEQ-UAS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RPM2 = namedtuple('RPM2', [x[0] for x in resp[1:]])
    # TSI
    resp = 'TSI11-0001-A::HWV=81.5516 RL,SN=KL5074443,DT=960607,ALMPF=1:PST=IS-NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    TSI6 = namedtuple('TSI6', [x[0] for x in resp[1:]])
    # OPM
    # OPM2
    resp = 'OPM3-W-0247:::PST=OOS-ANR-UEQ'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    OPM2 = namedtuple('OPM2', [x[0] for x in resp[1:]])
    # OPM9
    resp = 'OPM3-W-0241:TYPE=SHORT:HWV=81.5542A\rRI,SN=TZ3090137,DT=051105,ALMPF=1,CONN=SC,WLENGTH=1310:PST=IS-NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    OPM9 = namedtuple('OPM9', [x[0] for x in resp[1:]])
    # RMM7
    resp = 'RMM-0241:TYPE=MM3T:HWV=81.5544T RM,SN=UZ2685764,DT=060927,ALMPF=1:PST=IS-NR-STBY'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RMM7 = namedtuple('RMM7', [x[0] for x in resp[1:]])
    # RMM2
    resp = 'RMM-0253:::PST=OOS-ANR-UEQ-UAS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RMM2 = namedtuple('RMM2', [x[0] for x in resp[1:]])
    # MM7
    resp = 'MM-0247:TYPE=MM3T:HWV=81.5544T RN,SN=XZ0840259,DT=090421,ALMPF=1:PST=OOS-NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    MM7 = namedtuple('MM7', [x[0] for x in resp[1:]])
    # MM2
    resp = 'MM-0250:::PST=OOS-ANR-UEQ-UAS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    MM2 = namedtuple('MM2', [x[0] for x in resp[1:]])
    # Numeric only (####)
    # NN20
    # changed ####-##:DGR: to ####-##:TYPE=DGR:
    resp = '0002-01:TYPE=DGR:PMAID=PM1-0002-01,TACC=000,IDLECDE=AIS,OOSCDE=AIS,LINECDE=AMI,FMT=SF,EQLZ=1,GOS=0,ALM=INH,ALMPF=1,FENDPMTYPE=ANSI403,DS1ADDR=C,CSUADDR=B,TMG=THRU,FLTRC=NONE,PASUSP=OFF,PARTNAME=UASPART:PST=ISNR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    NN20 = namedtuple('NN20', [x[0] for x in resp[1:]])
    # NN19
    resp = "0111:TYPE=DGR:PMAID=PM3-0111,OOSCDE=AIS,FMT=M23,EQLZ=1,GOS=1,ALM=ALW,ALMPF=1,XBIT=YELLOW,AISTYPE=SST,BERTYPE=BPV,PARITY=PATH,MAP=ASYNC,FLTPR=DISABLED,FLTRC=NONE,PASUSP=OFF,PARTNAME=UASPART:PST=IS-NR'"
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    NN19 = namedtuple('NN19', [x[0] for x in resp[1:]])
    # NN8
    resp = '0095:TYPE=11A:HWV=81.5514A RG,SN=MS9902181,DT=981110,ALMPF=1,PRTN=ALW:PST=ISNR-STBY"'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    NN8 = namedtuple('NN8', [x[0] for x in resp[1:]])
    # NN6
    # resp = '0001-26,0113-14:CCT=2WAY,ALTMAP=NO,CCSTATE=IDLE,TAP=00'
    # resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    # resp = [x.split('=') for x in resp]
    # NN6 = namedtuple('NN6', [x[0] for x in resp[1:]])
    # SC8
    resp = 'B:TYPE=STS1E:HWV=81.5512B RF,SN=LI0292684,DT=980921:PST=IS-NR-STBY"'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    SC8 = namedtuple('SC8', [x[0] for x in resp[1:]])
    # STS13
    resp = 'STS1-0161::PMAID=PME-0161,TACC=000,GOS=1,ALM=ALW,ALMPF=1,STSMAP=VTFLOAT,PSL=VTFLOAT,PSLT=VTFLOAT,FLTPR=DISABLED,PASUSP=OFF,PARTNAME=UASPART:PST=ISNR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    STS13 = namedtuple('STS13', [x[0] for x in resp[1:]])
    # STS5
    resp = 'STS1-0001:ERCDE=SSNC,AID=PGC1-0001,PRMTR=AID&TYPE,TYPE=11'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    STS5 = namedtuple('STS5', [x[0] for x in resp[1:]])
    # STS4
    resp = 'STS1-0265:ERCDE=SSNV,AID=PGCO3-0265,PST=UAS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    STS4 = namedtuple('STS4', [x[0] for x in resp[1:]])
    # STS3
    resp = 'STS1-0194::PARTNAME=UASPART:PST=OOS-ANR-UAS'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    STS3 = namedtuple('STS3', [x[0] for x in resp[1:]])
    # RD7
    resp = 'RD,SN=HL1966661,DT=931213,ALMPF=1,PRTN=ALW,PMPI=PASSIVE:PST=IS-NR-ACT"'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RD7 = namedtuple('RD7', [x[0] for x in resp[1:]])
    # CSM6
    resp = 'CSM-12-A::HWV=81.5508 RN,ALMPF=1,SN=MI0039749,DT=980217:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    CSM6 = namedtuple('CSM6', [x[0] for x in resp[1:]])
    # ESM6
    resp = 'ESM-0161-A::HWV=81.5509 RT,ALMPF=99,SN=HL1793911,DT=940726:PST=IS-NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    ESM6 = namedtuple('ESM6', [x[0] for x in resp[1:]])
    # SIM7
    resp = 'SIM-B::HWV=81.5507B RA,ALMPF=99,TMG=SLV,SN=OI0613947,DT=000727:PST=IS-NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    SIM7 = namedtuple('SIM7', [x[0] for x in resp[1:]])
    # AIM6
    resp = 'AIM-A::HWV=81.5504 RH,ALMPF=5,SN=HL1927862,DT=931115:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    AIM6 = namedtuple('AIM6', [x[0] for x in resp[1:]])
    # SCM6
    resp = 'SCM-1-B::HWV=81.5506A RQ,ALMPF=5,SN=SZ3530843,DT=041220:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    SCM6 = namedtuple('SCM6', [x[0] for x in resp[1:]])
    # LSM6
    resp = 'LSM-A::HWV=81.55103 RC,ALMPF=99,SN=5A4160276 ,DT=150123:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    LSM6 = namedtuple('LSM6', [x[0] for x in resp[1:]])
    # ESS4
    resp = 'ESSPS-04-B::HWV=8X.5535,ALMPF=1:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    ESS4 = namedtuple('ESS4', [x[0] for x in resp[1:]])
    # CSS4
    resp = 'CSSPS-1-A::HWV=8X.5535,ALMPF=1:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    CSS4 = namedtuple('CSS4', [x[0] for x in resp[1:]])
    # ACP4
    resp = 'ACPS-2-B::HWV=8X.5534,ALMPF=5:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    ACP4 = namedtuple('ACP4', [x[0] for x in resp[1:]])
    # RG7
    resp = 'RG,ALMPF=5,SN=SD2610462,DT=040920,RAM=128MB,MACADDR2=08003E2F3CBC:PST=NR"'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    RG7 = namedtuple('RG7', [x[0] for x in resp[1:]])
    # LIP
    resp = 'LIP-B::HWV=81.55130RA,ALMPF=99:PST=NR'
    resp = [x for x in chain(*[r.split(',') for r in re.sub(":+", ":", resp).split(':')])]
    resp = [x.split('=') for x in resp]
    LIP = namedtuple('LIP', [x[0] for x in resp[1:]])

    candr = []
    for x in lo_tl1:
        try:
            if x[0] == ">":
                continue
            if x[0].count('=') == 0:
                continue
            elif x[0].startswith(TID):
                continue
            elif 'COMPLD' in x[0]:
                continue
            elif '/*' in x[0]:
                continue
            elif x[0].startswith("M "):
                continue
            else:
                candr.append(x[0])
        except:
            # print(x)
            pass
    do_resp = {}
    for tl1 in candr:
        resp, response = scrub_response(tl1)
        if resp[0][0] not in do_resp:
            try:
                if re.match(r'^PG', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = PGC6(*[x[1] for x in resp[1:]])
                    elif len(resp) == 5:
                        do_resp[resp[0][0]] = PGC5(*[x[1] for x in resp[1:]])
                    elif len(resp) == 4:
                        do_resp[resp[0][0]] = PGC4(*[x[1] for x in resp[1:]])
                    elif len(resp) == 3:
                        do_resp[resp[0][0]] = PGC3(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^PM', response):
                    if len(resp) == 9:
                        do_resp[resp[0][0]] = PM9(*[x[1] for x in resp[1:]])
                    elif len(resp) == 4:
                        do_resp[resp[0][0]] = PM4(*[x[1] for x in resp[1:]])
                    elif len(resp) == 2:
                        do_resp[resp[0][0]] = PM2(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^PP', response):
                    if len(resp) == 4:
                        do_resp[resp[0][0]] = PP4(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^RPM', response):
                    if len(resp) == 10:
                        do_resp[resp[0][0]] = RPM10(*[x[1] for x in resp[1:]])
                    elif len(resp) == 8:
                        do_resp[resp[0][0]] = RPM8(*[x[1] for x in resp[1:]])
                    elif len(resp) == 2:
                        do_resp[resp[0][0]] = RPM2(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^TSI', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = TSI6(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^OPM', response):
                    if len(resp) == 9:
                        do_resp[resp[0][0]] = OPM9(*[x[1] for x in resp[1:]])
                    elif len(resp) == 2:
                        do_resp[resp[0][0]] = OPM2(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^RMM-', response):
                    if len(resp) == 7:
                        do_resp[resp[0][0]] = RMM7(*[x[1] for x in resp[1:]])
                    elif len(resp) == 2:
                        do_resp[resp[0][0]] = RMM2(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^MM-', response):
                    if len(resp) == 7:
                        do_resp[resp[0][0]] = MM7(*[x[1] for x in resp[1:]])
                    elif len(resp) == 2:
                        do_resp[resp[0][0]] = MM2(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                if re.match(r'^[0-9]{4}', response):
                    if len(resp) == 20:
                        do_resp[resp[0][0]] = NN20(*[x[1] for x in resp[1:]])
                    if len(resp) == 19:
                        do_resp[resp[0][0]] = NN19(*[x[1] for x in resp[1:]])
                    if len(resp) == 8:
                        do_resp[resp[0][0]] = NN8(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^RD', response):
                    if len(resp) == 7:
                        do_resp[resp[0][0]] = RD7(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^CSM', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = CSM6(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^ESM', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = ESM6(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^SIM', response):
                    if len(resp) == 7:
                        do_resp[resp[0][0]] = SIM7(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^AIM', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = AIM6(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^SCM', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = SCM6(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^LSM', response):
                    if len(resp) == 6:
                        do_resp[resp[0][0]] = LSM6(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^ESS', response):
                    if len(resp) == 4:
                        do_resp[resp[0][0]] = ESS4(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^CSS', response):
                    if len(resp) == 4:
                        do_resp[resp[0][0]] = CSS4(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^ACP', response):
                    if len(resp) == 4:
                        do_resp[resp[0][0]] = ACP4(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^RG', response):
                    if len(resp) == 7:
                        do_resp[resp[0][0]] = RG7(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^[A-Z]:', response):
                    if len(resp[0][0]) == 1:
                        do_resp[resp[0][0]] = SC8(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^STS', response):
                    if len(resp) == 13:
                        do_resp[resp[0][0]] = STS13(*[x[1] for x in resp[1:]])
                    elif len(resp) == 5:
                        do_resp[resp[0][0]] = STS5(*[x[1] for x in resp[1:]])
                    elif len(resp) == 4:
                        do_resp[resp[0][0]] = STS4(*[x[1] for x in resp[1:]])
                    elif len(resp) == 3:
                        do_resp[resp[0][0]] = STS3(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                elif re.match(r'^LIP', response):
                    if len(resp) == 4:
                        do_resp[resp[0][0]] = LIP(*[x[1] for x in resp[1:]])
                    else:
                        print_exception(resp, response)
                else:
                    lo_skipped.append(response)
            except Exception as ex:
                print(ex.args)
                print(response)
                print(len(resp), resp)
                pass
        else:
            lo_dupes.append(resp)
    return do_resp


def circuits():
    """

    :return:
    """
    path = []
    for d in dcs:
        child = False
        if re.match(r'^[0-9]{3}-[0-9]{2}', dcs[d].slot):
            slot = '0' + dcs[d].slot
            if slot in do_resp:
                for t in t3z:
                    if t3z[t].ckt_id and t3z[t].ckt_id == dcs[d].child_circuit:
                        child = True
                        item = t3z[t].item
                        idx = item[0:item.index('.') + 1]
                        if idx in t3z:
                            level_0 = sorted([t3z[idx].aclli, t3z[idx].zclli])
                            level_1 = sorted([dcs[d].a_clli, dcs[d].z_clli])
                            path.append([level_1[0], level_0[0], level_0[1], level_1[1]])
                            if TID in level_0:
                                if path[0].index(TID) == 2:
                                    tl1_slot = '0' + dcs[d].slot
                                    [path[0].insert(-1, x) for x in
                                     [do_resp[tl1_slot].PMAID, dcs[d].services, tl1_slot]]
                                    print(
                                        f'{"T3Z":4} {dcs[d].slot:10} {path[0][0]} <--> {path[0][1]} <==> {path[0][2]}'
                                        f'<--|{path[0][3]}-({path[0][4]})-{path[0][5]}|--> {path[0][6]}')
                                    break
                                if path[0].index(TID) == 1:
                                    tl1_slot = '0' + dcs[d].slot
                                    [path[0].insert(1, x) for x in [do_resp[tl1_slot].PMAID, dcs[d].services, tl1_slot]]
                                    print(
                                        f'{"T3Z":4} {dcs[d].slot:10} {path[0][0]} <--|{path[0][1]}-({path[0][2]}'
                                        f'-{path[0][3]}| --> {path[0][4]} <= = > {path[0][5]} < --> {path[0][6]}'
                                    )
                                    break
                for s in st1:
                    path = []
                    if st1[s].ckt_id and st1[s].ckt_id == dcs[d].child_circuit:
                        child = True
                        item = st1[s].item
                        idx = item[0:item.index('.') + 1]
                        if idx in st1:
                            level_0 = sorted([st1[idx].aclli, st1[idx].zclli])
                            level_1 = sorted([dcs[d].a_clli, dcs[d].z_clli])
                            path.append([level_1[0], level_0[0], level_0[1], level_1[1]])
                            if TID in level_0:
                                if path[0].index(TID) == 2:
                                    tl1_slot = '0' + dcs[d].slot
                                    [path[0].insert(-1, x) for x in
                                     [do_resp[tl1_slot].PMAID, dcs[d].services, tl1_slot]]
                                    print(
                                        f'{"ST01":4} {dcs[d].slot:10} {path[0][0]} <--> {path[0][1]} <==> {path[0][2]}'
                                        f' <--|{path[0][3]}-({path[0][4]})-{path[0][5]}|--> {path[0][6]}')
                                    break
                                if path[0].index(TID) == 1:
                                    tl1_slot = '0' + dcs[d].slot
                                    [path[0].insert(1, x) for x in [do_resp[tl1_slot].PMAID, dcs[d].services, tl1_slot]]
                                    print(
                                        f'{"ST01":4} {dcs[d].slot:10} {path[0][0]} <--|{path[0][1]}-({path[0][2]})'
                                        f'-{path[0][3]}|--> {path[0][4]} <==> {path[0][5]} <--> {path[0][6]}')
                                    break
                for a in dcs:  # no child matches, now try and match the assignment to the local child_circuit
                    if dcs[d].assignment == dcs[a].child_circuit:
                        a2z_d = sorted([dcs[d].a_clli, dcs[d].z_clli])
                        a2z_a = sorted([dcs[a].a_clli, dcs[a].z_clli])
                        if a2z_d == a2z_a:
                            print(f'{"DCS":4} {dcs[d].slot:10} {a2z_d[0]} <==> {a2z_d[1]}')
                            break
                    if dcs[d].assignment and child == False:
                        print(f'{"DCS":4} {dcs[d].slot:10} {dcs[d].a_clli} <==> {dcs[d].z_clli}')
                        break


def build_xcon() -> List:
    """

    :return:
    """
    global dcs
    lo_cc = []
    xcon_file = data + 'tirks_xcon.csv'
    print(f'\n\nBuilding DCS cross connect from TIRKS... (be patient)')
    for fac in dcs:
        if re.match(r'^[0-9]{3}-[0-9]{2}', dcs[fac].slot):
            asg = dcs[fac].assignment
            cld = dcs[fac].child_circuit
            for f in dcs:
                if re.match(r'^[0-9]{3}-[0-9]{2}', dcs[f].slot):
                    if dcs[fac].slot != dcs[f].slot:
                        if asg == dcs[f].assignment:
                            lo_cc.append(["AA", dcs[fac].slot, dcs[fac].assignment, dcs[f].assignment, dcs[f].slot])
                        if asg == dcs[f].child_circuit:
                            lo_cc.append(["AC", dcs[fac].slot, dcs[fac].assignment, dcs[f].child_circuit, dcs[f].slot])
    print(f'Writing {xcon_file}')
    with open(xcon_file, 'w') as x_file:
        x_writer = csv.writer(x_file)
        x_writer.writerow(['Type', 'A Facility', 'A Circuit ID', 'Z Circuit ID', 'Z Facility'])
        for xcon in lo_cc:
            x_writer.writerow(xcon)
    return lo_cc


def main():
    global dcs
    global st1
    global t3z
    global do_resp
    dcs, st1, t3z, dcs_header, st1_header, t3z_header = process_dcs()
    do_resp = process_tl1()


    lo_fc = []
    for facility in dcs:
        if re.match(r'^[0-9]{3}-[0-9][0-9]', facility):
            lo_fc.append(facility)
    lo_fc.sort()
    print(f'\nTIRKS Source: BHLHPABEK31 TIRKS Data.xlsx')
    print(f'DCS: {len(dcs)} DT01: {len(st1)} T3Z: {len(t3z)}')
    print(f'Facilities: {len(lo_fc)} First: {lo_fc[0]}  Last: {lo_fc[-1]}')
    print(f'\nTL1 Source:{tl1_sources[0]}')
    print(f'TL1 responses read: {len(do_resp)}\n\n')

    circuits()
    xcon: List = build_xcon()


if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    main()
