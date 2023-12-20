import pandas as pd
from collections import OrderedDict

def atc_grouping_third(lookup_file, inferred_file, filename, choice="all"):

    if choice=="all":
        if lookup_file=="N/A" and inferred_file!="N/A":
            df = pd.read_csv(inferred_file)
        elif inferred_file=="N/A" and lookup_file!="N/A":
            df = pd.read_csv(lookup_file)
        elif lookup_file=="N/A" and inferred_file=="N/A":
            print("Both Files empty")
            return
        else:
            df_lookup = pd.read_csv(lookup_file)
            df_inferred = pd.read_csv(inferred_file)
            frames = [df_lookup, df_inferred]
            df = pd.concat(frames)
    elif choice=="lookup":
        df = pd.read_csv(lookup_file)
    elif choice=="inferred":
        df = pd.read_csv(inferred_file)

    df.to_csv(filename+'_Grouped.csv', index=False)
    atc_groups = {}

    check_rows= []
    for idi, i in df.iterrows():
        if isinstance(i['ATC_Code_3rd_Level'], float):
            if "OTHER" not in atc_groups.keys():
                atc_groups["OTHER"] = [1, [i["Drug_ID"]], [i["Drug_Name"]], [[i["sugeno_score"], i["Sugeno_Conf_Clin"],i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]]]
            elif i["Drug_ID"] not in atc_groups["OTHER"][1]:
                atc_groups["OTHER"][0] += 1
                atc_groups["OTHER"][1].extend([i["Drug_ID"]])
                atc_groups["OTHER"][2].extend([i["Drug_Name"]])
                atc_groups["OTHER"][3].extend([[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]])

        elif '[' in i['ATC_Code_3rd_Level']:
            check_rows.append(i)

        else:
            key = i['ATC_Code_3rd_Level']
            if key not in atc_groups.keys():
                atc_groups[key] = [1, [i["Drug_ID"]], [i["Drug_Name"]], [[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]], i["ATC_Code_3rd_Level_Names"]]

            elif i["Drug_ID"] not in atc_groups[key][1]:
                atc_groups[key][0] += 1
                atc_groups[key][1].extend([i["Drug_ID"]])
                atc_groups[key][2].extend([i["Drug_Name"]])
                atc_groups[key][3].extend([[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]])

    for idi, i in enumerate(check_rows):

        atc_codes_strings = i[4].rstrip(']').lstrip('[').split("', '")
        atc_codes_strings = [j.lstrip("'").rstrip("'") for j in atc_codes_strings]
        atc_codes_strings = list(OrderedDict.fromkeys(atc_codes_strings))

        df_atc = pd.read_csv('ATC level 3.csv')
        atc_codes_strings_names = []
        for p in atc_codes_strings:
            atc_codes_strings_names.append(df_atc.loc[df_atc['level3'] == p]['level3_description'].values[0])

        # atc_codes_counts = [atc_groups[w][0] if w in atc_groups.keys() else 0 for w in atc_codes_strings]
        # max_count, max_count_idx = max(atc_codes_counts), atc_codes_counts.index(max(atc_codes_counts))
        #
        # if max_count == 0:
        #     atc_groups[atc_codes_strings[max_count_idx]] = [1, [i[1]], [i[2]], [[i[6], i[7], i[8], i[9]]],
        #                                                     atc_codes_strings_names[max_count_idx]]
        # else:
        #     atc_groups[atc_codes_strings[max_count_idx]][0] += 1
        #     atc_groups[atc_codes_strings[max_count_idx]][1].extend([i[1]])
        #     atc_groups[atc_codes_strings[max_count_idx]][2].extend([i[2]])
        #     atc_groups[atc_codes_strings[max_count_idx]][3].extend([[i[6], i[7], i[8], i[9]]])

        for idk, key in enumerate(atc_codes_strings):
            if key not in atc_groups.keys():
                # print(i)
                # print(key, idk)
                # print(atc_codes_strings)
                # print(atc_codes_strings_names)
                # print("\n")
                atc_groups[key] = [1, [i[1]], [i[2]],[[i[6], i[13], i[7], i[8], i[9]]], atc_codes_strings_names[idk]]

            elif i["Drug_ID"] not in atc_groups[key][1]:
                atc_groups[key][0] += 1
                atc_groups[key][1].extend([i[1]])
                atc_groups[key][2].extend([i[2]])
                atc_groups[key][3].extend([[i[6], i[13], i[7], i[8], i[9]]])

    group_entries = []


    for idc, c in enumerate(atc_groups.keys()):
        scores = atc_groups[c][3]
        if c=="OTHER":
            atc_groups_name = "ATC_GROUP_NOT_FOUND"
        else:
            atc_groups_name = atc_groups[c][4]
        max_score = [0,0,0,0]
        max_score_drug_id = ""
        max_score_drug_name = ""
        for idd, d in enumerate(scores):
            if (d[0]>max_score[0]) or (d[0]==max_score[0] and d[1]>max_score[1]) or (d[0]==max_score[0] and d[1]==max_score[1] and d[2]>max_score[2]) or (d[0]==max_score[0] and d[1]==max_score[1] and d[2]==max_score[2] and d[3]>max_score[3]):
                max_score = d
                max_score_drug_id = atc_groups[c][1][idd]
                max_score_drug_name = atc_groups[c][2][idd]

        for idd, d in enumerate(scores):
            if idd == 0:
                row = {
                    "ATC_CODE_THIRD_LEVEL": c,
                    "ATC_COD_THIRD_LEVEL_NAME": atc_groups_name,
                    "DRUG ID": atc_groups[c][1][idd],
                    "DRUG NAME": atc_groups[c][2][idd],
                    "DRUG SCORES": d,
                    "DRUG ID WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": max_score_drug_id,
                    "DRUG NAME WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": max_score_drug_name,
                    "HIGHEST SCORES FOR THIS CODE (WITHIN THE RESULTS) (ORDER: SUGENO, CONFIDENCE, NOVELTY, CLINICAL": max_score
                }
            else:
                row = {
                    "ATC_CODE_THIRD_LEVEL": "",
                    "ATC_COD_THIRD_LEVEL_NAME": "",
                    "DRUG ID": atc_groups[c][1][idd],
                    "DRUG NAME": atc_groups[c][2][idd],
                    "DRUG SCORES": d,
                    "DRUG ID WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": "",
                    "DRUG NAME WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": "",
                    "HIGHEST SCORES FOR THIS CODE (WITHIN THE RESULTS) (ORDER: SUGENO, CONFIDENCE, NOVELTY, CLINICAL": ""
                }

            group_entries.append(row)

    df_group = pd.DataFrame(group_entries)
    df_group.to_csv(f"{filename}_Grouped_ATC_3rd.csv", index=False)

def atc_grouping_second(lookup_file, inferred_file, filename, choice="all"):
    if choice == "all":
        if lookup_file == "N/A" and inferred_file != "N/A":
            df = pd.read_csv(inferred_file)
        elif inferred_file == "N/A" and lookup_file != "N/A":
            df = pd.read_csv(lookup_file)
        elif lookup_file == "N/A" and inferred_file == "N/A":
            print("Both Files empty")
            return
        else:
            df_lookup = pd.read_csv(lookup_file)
            df_inferred = pd.read_csv(inferred_file)
            frames = [df_lookup, df_inferred]
            df = pd.concat(frames)
    elif choice == "lookup":
        df = pd.read_csv(lookup_file)
    elif choice == "inferred":
        df = pd.read_csv(inferred_file)
    df.to_csv(filename+'_Grouped.csv', index=False)
    atc_groups = {}
    # df = df.iloc[:, 1:]

    check_rows= []
    for idi, i in df.iterrows():
        if isinstance(i['ATC_Code_2nd_Level'], float):
            if "OTHER" not in atc_groups.keys():
                atc_groups["OTHER"] = [1, [i["Drug_ID"]], [i["Drug_Name"]], [[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]]]
            elif i["Drug_ID"] not in atc_groups["OTHER"][1]:
                atc_groups["OTHER"][0] += 1
                atc_groups["OTHER"][1].extend([i["Drug_ID"]])
                atc_groups["OTHER"][2].extend([i["Drug_Name"]])
                atc_groups["OTHER"][3].extend([[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]])

        elif '[' in i['ATC_Code_2nd_Level']:
            check_rows.append(i)

        else:
            key = i['ATC_Code_2nd_Level']
            if key not in atc_groups.keys():
                atc_groups[key] = [1, [i["Drug_ID"]], [i["Drug_Name"]], [[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]], i["ATC_Code_3rd_Level_Names"]]

            elif i["Drug_ID"] not in atc_groups[key][1]:
                atc_groups[key][0] += 1
                atc_groups[key][1].extend([i["Drug_ID"]])
                atc_groups[key][2].extend([i["Drug_Name"]])
                atc_groups[key][3].extend([[i["sugeno_score"], i["Sugeno_Conf_Clin"], i["comp_confidence_score"], i["comp_novelty_score"], i["comp_clinical_evidence_score"]]])

    for idi, i in enumerate(check_rows):
        atc_codes_strings = i[10].rstrip(']').lstrip('[').split("', '")
        atc_codes_strings = [j.lstrip("'").rstrip("'") for j in atc_codes_strings]
        atc_codes_strings = list(OrderedDict.fromkeys(atc_codes_strings))

        atc_codes_strings_names = i[11].rstrip(']').lstrip('[').split("', '")
        atc_codes_strings_names = [j.lstrip("'").rstrip("'") for j in atc_codes_strings_names]
        atc_codes_strings_names = list(OrderedDict.fromkeys(atc_codes_strings_names))

        atc_codes_counts = [atc_groups[w][0] if w in atc_groups.keys() else 0 for w in atc_codes_strings]
        max_count, max_count_idx = max(atc_codes_counts), atc_codes_counts.index(max(atc_codes_counts))

        if max_count==0:
            atc_groups[atc_codes_strings[max_count_idx]] = [1, [i[1]], [i[2]], [[i[6], i[14], i[7], i[8], i[9]]], atc_codes_strings_names[max_count_idx]]
        else:
            atc_groups[atc_codes_strings[max_count_idx]][0] += 1
            atc_groups[atc_codes_strings[max_count_idx]][1].extend([i[1]])
            atc_groups[atc_codes_strings[max_count_idx]][2].extend([i[2]])
            atc_groups[atc_codes_strings[max_count_idx]][3].extend([[i[6], i[14], i[7], i[8], i[9]]])

        # for idk, key in enumerate(atc_codes_strings):
        #     if key not in atc_groups.keys():
        #         # print(i)
        #         # print(key, idk)
        #         # print(atc_codes_strings)
        #         # print(atc_codes_strings_names)
        #         # print("\n")
        #         atc_groups[key] = [1, [i[3]], [i[4]],[[i[8], i[9], i[10], i[11]]], atc_codes_strings_names[idk]]
        #
        #     elif i["Drug_ID"] not in atc_groups[key][1]:
        #         atc_groups[key][0] += 1
        #         atc_groups[key][1].extend([i[3]])
        #         atc_groups[key][2].extend([i[4]])
        #         atc_groups[key][3].extend([[i[8], i[9], i[10], i[11]]])

    group_entries = []
    group_entries_new = []


    for c in atc_groups.keys():
        scores = atc_groups[c][3]
        if c=="OTHER":
            atc_groups_name = "ATC_GROUP_NOT_FOUND"
        else:
            atc_groups_name = atc_groups[c][4]
        max_score = [0,0,0,0,0]
        max_score_drug_id = ""
        max_score_drug_name = ""
        for idd, d in enumerate(scores):
            if (d[1]>max_score[1]) or (d[1]==max_score[1] and d[2]>max_score[2]) or (d[1]==max_score[1] and d[2]==max_score[2] and d[4]>max_score[4]):
                max_score = d
                max_score_drug_id = atc_groups[c][1][idd]
                max_score_drug_name = atc_groups[c][2][idd]
        row_new = {
            "ATC_CODE_SECOND_LEVEL": c,
            "ATC_COD_SECOND_LEVEL_NAME": atc_groups_name,
            "DRUG ID WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": max_score_drug_id,
            "DRUG NAME WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": max_score_drug_name,
            "ORDERING SCORE (ONLY CLINICAL AND CONFIDENCE)": max_score[1],
            "CONFIDENCE SCORE": max_score[2],
            "CLINICAL SCORE": max_score[4]
        }

        group_entries_new.append(row_new)

        for idd, d in enumerate(scores):
            if idd == 0:
                row = {
                    "ATC_CODE_SECOND_LEVEL": c,
                    "ATC_COD_SECOND_LEVEL_NAME": atc_groups_name,
                    "DRUG ID": atc_groups[c][1][idd],
                    "DRUG NAME": atc_groups[c][2][idd],
                    "DRUG ORDERING SCORE (ONLY CLINICAL AND CONFIDENCE)": d[1],
                    "DRUG CONFIDENCE SCORE": d[2],
                    "DRUG CLINICAL SCORE": d[4],
                    "DRUG ID WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": max_score_drug_id,
                    "DRUG NAME WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": max_score_drug_name,
                    "ORDERING SCORE (ONLY CLINICAL AND CONFIDENCE) FOR DRUG WITH HIGHEST SCORE FOR THIS CODE": max_score[1],
                    "CONFIDENCE SCORE FOR DRUG WITH HIGHEST SCORE FOR THIS CODE": max_score[2],
                    "CLINICAL SCORE FOR DRUG WITH HIGHEST SCORE FOR THIS CODE": max_score[4]
                }
            else:
                row = {
                    "ATC_CODE_SECOND_LEVEL": "",
                    "ATC_COD_SECOND_LEVEL_NAME": "",
                    "DRUG ID": atc_groups[c][1][idd],
                    "DRUG NAME": atc_groups[c][2][idd],
                    "DRUG ORDERING SCORE (ONLY CLINICAL AND CONFIDENCE)": d[1],
                    "DRUG CONFIDENCE SCORE": d[2],
                    "DRUG CLINICAL SCORE": d[4],
                    "DRUG ID WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": "",
                    "DRUG NAME WITH HIGHEST SCORE FOR THIS CODE (WITHIN THE RESULTS)": "",
                    "ORDERING SCORE (ONLY CLINICAL AND CONFIDENCE) FOR DRUG WITH HIGHEST SCORE FOR THIS CODE": "",
                    "CONFIDENCE SCORE FOR DRUG WITH HIGHEST SCORE FOR THIS CODE": "",
                    "CLINICAL SCORE FOR DRUG WITH HIGHEST SCORE FOR THIS CODE": ""
                }
            group_entries.append(row)

    df_group = pd.DataFrame(group_entries)
    df_group_new = pd.DataFrame(group_entries_new)
    df_group.to_csv(f"{filename}_Grouped_ATC_2nd.csv", index=False)
    df_group_new.to_csv(f"{filename}_ATC_2nd_Only.csv", index=False)