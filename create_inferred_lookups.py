import requests
import json
import pandas as pd
from datetime import date
import time
import os
from grouping_atc import atc_grouping_third, atc_grouping_second
from sugeno_functions import compute_sugeno
import numpy as np


def grouping(mergedResponse, grouping="ATC"):

    if grouping=="ATC":
        df = pd.read_csv('ATC level 3.csv')
        df2 = pd.read_csv('ATC level 2.csv')

        df_infores = pd.read_csv('InfoRes_Catalog.csv')

        results = mergedResponse['fields']['data']['message']['results']
        edges = mergedResponse['fields']['data']['message']['knowledge_graph']['edges']
        nodes = mergedResponse['fields']['data']['message']['knowledge_graph']['nodes']

        lookup_res = []
        inferred_res = []

        inferred_res_kl = []

        for idi, i in enumerate(range(df_infores.shape[0])):
            if df_infores.iloc[idi, 1] == "curated":
                lookup_res.append(df_infores.iloc[idi, 0])
            else:
                inferred_res.append(df_infores.iloc[idi, 0])
                inferred_res_kl.append(df_infores.iloc[idi, 1])

        lookup = []
        inferred = []
        full = []

        for idres, res in enumerate(results):
            sugeno = round(res['sugeno'],2)
            comp_novelty = round(res['ordering_components']['novelty'], 2)
            comp_confidence = round(res['ordering_components']['confidence'], 2)
            comp_clinical_evidence = round(res['ordering_components']['clinical_evidence'], 2)
            # ara_scores = [[anal['score'], anal['resource_id']] for anal in res['analyses']]
            improving_agent = False
            improving_agent_score = -.0001

            ARAX = False
            ARAX_score = -.0001

            unsecret = False
            unsecret_score = -.0001

            biothings_explorer = False
            biothings_explorer_score = -.0001

            aragorn = False
            aragorn_score = -.0001


            for analysis in res['analyses']:
                if analysis['resource_id'] == "infores:improving-agent":
                    improving_agent = True
                    improving_agent_score = analysis['score']

                elif analysis['resource_id'] == "infores:rtx-kg2":
                    ARAX = True
                    ARAX_score = analysis['score']

                elif analysis['resource_id'] == "infores:biothings-explorer":
                    biothings_explorer = True
                    biothings_explorer_score = analysis['score']

                elif analysis['resource_id'] == "infores:unsecret-agent":
                    unsecret = True
                    unsecret_score = analysis['score']

                elif analysis['resource_id'] == "infores:aragorn":
                    aragorn = True
                    aragorn_score = analysis['score']


            sugeno_conf_clin = round(compute_sugeno(score_confidence= comp_confidence, score_novelty=comp_novelty,
                                                    score_clinical=comp_clinical_evidence, weight_novelty=0.0)[2], 2)
            sugeno_conf_clin_1_5 = round(compute_sugeno(score_confidence=comp_confidence, score_novelty=comp_novelty,
                                                    score_clinical=comp_clinical_evidence, weight_novelty=0.0,
                                                        weight_confidence=1.0, weight_clinical=0.5)[2], 2)
            sugeno_conf_clin_5_1 = round(compute_sugeno(score_confidence=comp_confidence, score_novelty=comp_novelty,
                                                        score_clinical=comp_clinical_evidence, weight_novelty=0.0,
                                                        weight_confidence=0.5, weight_clinical=1.0)[2], 2)
            sugeno_conf_clin_5_5 = round(compute_sugeno(score_confidence=comp_confidence, score_novelty=comp_novelty,
                                                        score_clinical=comp_clinical_evidence, weight_novelty=0.0,
                                                        weight_confidence=0.8, weight_clinical=0.8)[2], 2)
            for anal in res['analyses']:
                is_lookup = 0
                edge_id = anal['edge_bindings']['t_edge'][0]['id']
                prim_src_found = 0

                for t in edges[edge_id]['sources']:
                    if t['resource_role'] == 'primary_knowledge_source':
                        if t['resource_id'] not in inferred_res:
                            is_lookup = 1
                            knowledge_level = "curated"
                        else:
                            knowledge_level = inferred_res_kl[inferred_res.index(t['resource_id'])]
                        prim_source = t['resource_id']
                        prim_src_found = 1

                if prim_src_found ==0:
                    prim_source = "Not Found"
                    knowledge_level = "Not Found"

                obj = edges[edge_id]['object']
                subj = edges[edge_id]['subject']

                if 'MONDO' in obj:
                    node = subj
                else:
                    node = obj

                if 'name' in nodes[node].keys():
                    node_name = nodes[node]['name']
                else:
                    node_name = "NO NAME GIVEN"

                found=0
                if nodes[node]['attributes'] != [] :
                    for idi, i in enumerate(nodes[node]['attributes']):
                        if i['attribute_type_id'] == 'biothings_annotations':
                            found = 1
                            break
                    if found ==1:
                        if 'chembl' in nodes[node]['attributes'][idi]['value'][0].keys():
                            if isinstance(nodes[node]['attributes'][idi]['value'][0]['chembl'], dict):
                                if 'atc_classifications' in nodes[node]['attributes'][idi]['value'][0]['chembl'].keys():
                                    atc_code = nodes[node]['attributes'][idi]['value'][0]['chembl']['atc_classifications']
                                else:
                                    atc_code = "N/A"
                            elif isinstance(nodes[node]['attributes'][idi]['value'][0]['chembl'], list):
                                atc_found = 0
                                atc_code = []
                                for j in nodes[node]['attributes'][idi]['value'][0]['chembl']:
                                    if 'atc_classifications' in j.keys():
                                        atc_code.append(j['atc_classifications'])
                                        atc_found = 1
                                if atc_found==0:
                                    atc_code = "N/A"
                        else:
                            atc_code = "N/A"
                    else:
                        atc_code = "N/A"
                else:
                    atc_code = "N/A"

                if isinstance(atc_code, str):
                    atc_3 = ""
                    atc_3_name = ""
                    atc_3 = ''.join(atc_code.split())[:-3].upper()

                    atc_2 = ""
                    atc_2_name = ""
                    atc_2 = ''.join(atc_code.split())[:-4].upper()

                    if atc_3 !="":
                        atc_3_name = df.loc[df['level3']==atc_3]['level3_description'].values[0]
                    else:
                        atc_3 = "N/A"
                        atc_3_name = "N/A"

                    if atc_2 != "":
                        atc_2_name = df2.loc[df2['level2'] == atc_2]['level2_description'].values[0]
                    else:
                        atc_2 = "N/A"
                        atc_2_name = "N/A"

                else:
                    atc_3, atc_3_name = [], []

                    for i in atc_code:
                        atc_3_str = ''.join(i.split())[:-3].upper()
                        atc_3.append(atc_3_str)
                        atc_3_name.append(df.loc[df['level3']==atc_3_str]['level3_description'].values[0])

                    atc_2, atc_2_name = [], []
                    for i in atc_code:
                        atc_2_str = ''.join(i.split())[:-4].upper()
                        atc_2.append(atc_2_str)
                        atc_2_name.append(df2.loc[df2['level2'] == atc_2_str]['level2_description'].values[0])

                row = {
                    'Result_Num': idres,
                    'Drug_ID': node,
                    'Drug_Name': node_name,
                    'ATC_Code': atc_code,
                    'ATC_Code_3rd_Level': atc_3,
                    'ATC_Code_3rd_Level_Names': atc_3_name,
                    'sugeno_score': sugeno,
                    'comp_confidence_score': comp_confidence,
                    'comp_novelty_score': comp_novelty,
                    'comp_clinical_evidence_score': comp_clinical_evidence,
                    'ATC_Code_2nd_Level': atc_2,
                    'ATC_Code_2nd_Level_Names': atc_2_name,
                    'Knowledge Level': knowledge_level,
                    'Rank': res['rank'],
                    'Sugeno_Conf_Clin': sugeno_conf_clin,
                    'Sugeno_Conf_Clin_1_5': sugeno_conf_clin_1_5,
                    'Sugeno_Conf_Clin_5_1': sugeno_conf_clin_5_1,
                    'Sugeno_Conf_Clin_5_5': sugeno_conf_clin_5_5,
                    'ARAX': ARAX,
                    'ARAX_score': ARAX_score,
                    'unsecret': unsecret,
                    'unsecret_score': unsecret_score,
                    'improving_agent': improving_agent,
                    'improving_agent_score': improving_agent_score,
                    'biothings_explorer': biothings_explorer,
                    'biothings_explorer_score': biothings_explorer_score,
                    'aragorn': aragorn,
                    'aragorn_score': aragorn_score,
                }

                if is_lookup == 1:
                    if row not in lookup:
                        lookup.append(row)

                else:
                    if row not in inferred:
                        inferred.append(row)
                full.append(row)

        inferred_not_repeated= []
        for i in inferred:
            ffound=0
            for j in lookup:
                if list(i.values())[:-6] == list(j.values())[:-6]:
                    ffound=1
                    break
            if ffound==0:
                inferred_not_repeated.append(i)

        return lookup, inferred_not_repeated, full


disease_dict = {
                "MONDO:0005267": "Heart Disorder",
                # # "MONDO:0005709": "Common_Cold",
                # "MONDO:0011786": "Allergic Rhinitis",
                # # "MONDO:0005277": "Migraine Disorder",
                # # "MONDO:0005148": "Type 2 Diabetes",
                "MONDO:0004975": "Alzheimer Disease",
                # # "MONDO:0005100": "Systemic Sclerosis",
                # # "MONDO:0800044": "NGLY1-Deficiency",
                "MONDO:0016063": "Cowden Disease",
                "MONDO:0017314": "Ehlers-Danlos Disease",
                "MONDO:0010808": "Familial Insomnia",
                "MONDO:0018150": "Gaucher Disease",
                "MONDO:0020333": "Aggressive Systemic Mastocytosis",
                # # "MONDO:0000004": "Adrenocortical Hypofunction",
                # # "MONDO:0005002": "Chronic Obstructive Pulmonary Disease",
                # # "MONDO:0006936": "Pulmonary Stenosis",
                "MONDO:0011426": "Aceruloplasminemia",
                "MONDO:0007743": "attention deficit-hyperactivity disorder",
                "MONDO:0004784": "Allergic Asthma",
                "MONDO:0001505": "Alcoholic Hepatitis",
                # "MONDO:0014252": "Approved for Familial defective apolipoprotein B (APOB)",
                "MONDO:0007750": "Approved for Familial hypercholesterolemia heterozygous (LDLR)",
                "MONDO:0005260": "Autism",
                "MONDO:0015564": "Castleman Disease",
                "MONDO:0006497": "Cerebral palsy",
                "MONDO:0043233": "Exfoliative Dermatitis",
                "MONDO:0008251": "familial pityriasis rubra pilaris",
                "MONDO:0007186": "Gastroesophageal Reflux Disease",
                "MONDO:0005393": "Gout",
                "MONDO:0015364": "Hereditary Sensory And Autonomic Neuropathy Type",
                "MONDO:0009746": "Hereditary sensory and autonomic neuropathy type 4",
                "MONDO:0005799": "Hookworm Infectious Disease",
                "MONDO:0100345": "Lactose intolerance",
                "MONDO:0018493": "Malignant Hyperthermia Of Anesthesia",
                "MONDO:0005301": "Multiple Sclerosis",
                "MONDO:0018911": "Maturity-onset Diabetes Of The Young",
                "MONDO:0010794": "NARP Syndrome",
                "MONDO:0018958": "Nemaline myopathy",
                "MONDO:0011873": "Niemann-Pick disease, type C",
                "MONDO:0007147": "Obstructive Sleep Apnea",
                "MONDO:0004260": "Peptic Ulcer Perforation",
                "MONDO:0001119": "Premature Menopause",
                "MONDO:0019173": "Rabies",
                "MONDO:0004758": "scotoma",
                "MONDO:0019600": "Xeroderma Pigmentosum",
                "UMLS:C1689985": "Absence",
                "UMLS:C1961102": "Acute Lymphoblastic Leukemia",
                "UMLS:C0948216": "Adenocarcinoma of the Ovary",
                "UMLS:C5204526": "Advanced Adenocarcinoma",
                "UMLS:C0152069": "Alveolar echinococcosis",
                "UMLS:C0268407": "Amyloid cardiomyopathy",
                "UMLS:C0002726": "Amyloidosis",
                "UMLS:C0393911": "Autonomic failure",
                "UMLS:C0259749": "Autonomic neuropathy",
                "UMLS:C0007097": "Carcinomas",
                "UMLS:C1384641": "Cervical spondylosis",
                "UMLS:C0346421": "Chronic eosinophilic leukemia",
                "UMLS:C0009566": "Complication",
                "UMLS:C0010043": "corneal ulcers",
                "UMLS:C4551761": "Excessive daytime sleepiness",
                "UMLS:C0031069": "Familial Mediterranean Fever",
                "UMLS:C1456270": "fatty acid oxidation disorder",
                "UMLS:C0037054": "Hemoglobin S",
                "UMLS:C1328504": "Hormone refractory prostate cancer",
                "UMLS:C0205725": "Human cytomegalovirus",
                "UMLS:C0033300": "Hutchinson-Gilford Progeria Syndrome",
                "UMLS:C4553297": "Hydatid Disease",
                "UMLS:C1291314": "hydroxylase deficiency",
                "UMLS:C0020598": "Hypocalcemia",
                "UMLS:C0341862": "Hypothalamic amenorrhea",
                "UMLS:C0206141": "Idiopathic Hypereosinophilic Syndrome",
                "UMLS:C0024302": "large cell lymphoma",
                "UMLS:C0149921": "lead poisoning in children",
                "UMLS:C3554225": "LEPTIN RECEPTOR DEFICIENCY",
                "UMLS:C0023869": "Lithiasis",
                "UMLS:C0024523": "Malabsorption",
                "UMLS:C1384494": "Metastatic Carcinoma",
                "UMLS:C3538749": "Myeloma",
                "UMLS:C0854467": "Myelosuppression",
                "UMLS:C0027831": "Neurofibromatosis type 1",
                "UMLS:C0855139": "Nodal Marginal Zone Lymphoma",
                "UMLS:C1829844": "Organic acidemias",
                "UMLS:C0268130": "OROTIC ACIDURIA",
                "UMLS:C0030343": "Panuveitis",
                "UMLS:C0687150": "Parathyroid Carcinoma",
                "UMLS:C0039106": "Pigmented villonodular synovitis",
                "UMLS:C0278791": "Refractory CLL",
                "UMLS:C0085298": "SCD",
                "UMLS:C1959629": "Seizure",
                "UMLS:C0037933": "Spinal Diseases",
                "UMLS:C0442893": "Systemic disease",
                "UMLS:C0221013": "Systemic mastocytosis",
                "UMLS:C0278851": "Thyroid carcinoma",
                "UMLS:C4325554": "Traumatic anterior uveitis",
                "UMLS:C0040954": "whipworm"
                }
for tmp_str in ['CI', 'TEST']:
    if tmp_str=='CI':
        link = f"https://ars.ci.transltr.io/ars/api"
    else:
        link = f"https://ars.test.transltr.io/ars/api"
    row_rank_list = []
    for id_dis in disease_dict.keys():
        done=0
        name_dis = disease_dict[id_dis]
        submit_filename = f"{name_dis}_Submit_{date.today()}_{tmp_str}.json"
        if submit_filename not in os.listdir():
            query = {
                "message": {
                    "query_graph": {
                        "edges": {
                            "t_edge": {
                                "object": "on",
                                "subject": "sn",
                                "predicates": ["biolink:treats"]
                            }
                        },
                        "nodes": {
                            "on": {
                                "ids": [id_dis],
                                "categories": [
                                    "biolink:Disease"
                                ]
                            },
                            "sn": {
                                "categories": [
                                    "biolink:ChemicalEntity"
                                ]
                            }
                        }
                    }
                }
            }
            response = requests.post(link + f"/submit", json=query).json()
            with open(submit_filename, 'w') as f:
                json.dump(response, f)

        while True:
            with open(submit_filename, 'r') as f:
                id = json.load(f)['pk']
                response = requests.get(link + f"/messages/{id}?trace=y")
                data = response.json()
                print(data['status'])
                if data['status'] == 'Done' and data['merged_version'] != "None":
                    merged_version = data['merged_version']
                    print(f"For Disease {name_dis}, pk:{merged_version}")
                    url_merged_version = link + f'/messages/{merged_version}'
                    while True:
                        mergedResponse = requests.get(url_merged_version).json()
                        if mergedResponse['fields']['code'] == 200:
                            f_out = open(f"{name_dis}_response_{merged_version}_{tmp_str}.json", 'w')
                            json.dump(mergedResponse, f_out)


                            lookup_f, inferred_f = "N/A", "N/A"
                            lookup, inferred, full = grouping(mergedResponse)
                            excel_filename = f"{name_dis}_results_{date.today()}_{tmp_str}"

                            dis_name = disease_dict[id_dis]
                            df_bench = pd.read_csv('Benchmarks_12-7-2023.csv', header=0)
                            pos_bench = []
                            neg_bench = []


                            full_names = [[i['Drug_Name'], i['Knowledge Level'], i['Rank'], i['comp_confidence_score'],
                                           i['comp_clinical_evidence_score'], i['comp_novelty_score'], i['sugeno_score'],
                                           i['Sugeno_Conf_Clin'], i['Sugeno_Conf_Clin_1_5'], i['Sugeno_Conf_Clin_5_1'],
                                           i['Sugeno_Conf_Clin_5_5'], i['ARAX'], i['ARAX_score'], i['unsecret'], i['unsecret_score'],
                                           i['improving_agent'], i['improving_agent_score'], i['biothings_explorer'], i['biothings_explorer_score'],
                                           i['aragorn'], i['aragorn_score']] for i in lookup]
                            row_rank = {}
                            # row_rank_list = []
                            for index, row in df_bench.iterrows():
                                if row['Disease'] == dis_name:
                                    found = 0
                                    for c in full_names:
                                        if row['Drugs'].lower() == str(c[0]).lower():
                                            tmp = c[2]
                                            tmp_kl = c[1]
                                            tmp_conf = c[3]
                                            tmp_clin = c[4]
                                            tmp_nov = c[5]
                                            tmp_sug = c[6]
                                            tmp_sug1 = c[7]
                                            tmp_sug2 = c[8]
                                            tmp_sug3 = c[9]
                                            tmp_sug4 = c[10]
                                            tmp_arax = c[11]
                                            tmp_arax_score = c[12]
                                            tmp_unsecret = c[13]
                                            tmp_unsecret_score = c[14]
                                            tmp_improving = c[15]
                                            tmp_improving_score = c[16]
                                            tmp_biothings = c[17]
                                            tmp_biothings_score = c[18]
                                            tmp_aragorn = c[19]
                                            tmp_aragorn_score = c[20]

                                            found = 1
                                            break

                                    if found==0:
                                        tmp = "NOT AVAILABLE"
                                        tmp_kl = "NOT AVAILABLE"
                                        tmp_conf = "NOT AVAILABLE"
                                        tmp_clin = "NOT AVAILABLE"
                                        tmp_nov = "NOT AVAILABLE"
                                        tmp_sug = "NOT AVAILABLE"
                                        tmp_sug1 = "NOT AVAILABLE"
                                        tmp_sug2 = "NOT AVAILABLE"
                                        tmp_sug3 = "NOT AVAILABLE"
                                        tmp_sug4 = "NOT AVAILABLE"
                                        tmp_arax ="NOT AVAILABLE"
                                        tmp_arax_score = "NOT AVAILABLE"
                                        tmp_unsecret = "NOT AVAILABLE"
                                        tmp_unsecret_score = "NOT AVAILABLE"
                                        tmp_improving = "NOT AVAILABLE"
                                        tmp_improving_score = "NOT AVAILABLE"
                                        tmp_biothings = "NOT AVAILABLE"
                                        tmp_biothings_score = "NOT AVAILABLE"
                                        tmp_aragorn = "NOT AVAILABLE"
                                        tmp_aragorn_score = "NOT AVAILABLE"

                                    row_rank={
                                        'Disease' : row['Disease'],
                                        'Drugs': row['Drugs'],
                                        'Benchmark': row['Benchmark'],
                                        'Rank': tmp,
                                        'Knowledge Level': tmp_kl,
                                        'Confidence Score': tmp_conf,
                                        'Clinical Evidence Score': tmp_clin,
                                        'Novelty Score': tmp_nov,
                                        'Sugeno Score': tmp_sug,
                                        'Sugeno (Conf=1.0, Clinical=1.0)': tmp_sug1,
                                        'Sugeno (Conf=1.0, Clinical=0.5)': tmp_sug2,
                                        'Sugeno (Conf=0.5, Clinical=1.0)': tmp_sug3,
                                        'Sugeno (Conf=0.8, Clinical=0.8)': tmp_sug4,
                                        'ARAX': tmp_arax,
                                        'ARAX_score': tmp_arax_score,
                                        'unsecret': tmp_unsecret,
                                        'unsecret_score': tmp_unsecret_score,
                                        'improving_agent': tmp_improving,
                                        'improving_agent_score': tmp_improving_score,
                                        'biothings_explorer': tmp_biothings,
                                        'biothings_explorer_score': tmp_biothings_score,
                                        'aragorn': tmp_aragorn,
                                        'aragorn_score': tmp_aragorn_score,
                                    }
                                    row_rank_list.append(row_rank)

                            if len(inferred) > 0:
                                df_inferred = pd.DataFrame(inferred)
                                df_inferred.to_csv(excel_filename + '_Inferred.csv', index=False)
                                inferred_f = excel_filename + '_Inferred.csv'

                            if len(lookup) > 0:
                                df_lookup = pd.DataFrame(lookup)
                                df_lookup.to_csv(excel_filename + '_Lookup.csv', index=False)
                                lookup_f = excel_filename + '_Lookup.csv'
                                atc_grouping_second(lookup_f, inferred_f, excel_filename, choice="lookup")

                            done=1
                            break
                        elif mergedResponse['fields']['code'] == 202:
                            time.sleep(100)
                        else:
                            print("Merged Version Error!")
                            f_out = open(f"{name_dis}_response_{merged_version}_{tmp_str}.json", 'w')
                            json.dump(mergedResponse, f_out)
                            done=1
                            break

                elif data['status'] == 'Running':
                    time.sleep(100)
                else:
                    break

                if done==1:
                    break

df_bench_rank = pd.DataFrame(row_rank_list)
df_bench_rank.to_csv(f'Benchmarks_Rank_{date.today()}.csv', index=False)