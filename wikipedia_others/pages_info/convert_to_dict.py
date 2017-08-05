import glob

import json




def convert_to_dict_page_infos(parameters):
    input_file = "{0}/{1}/wpTitle_to_wpId/part*".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )

    #output_file = "/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json"
    output_file = "{0}/{1}/wpTitle_to_wpId.json".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )
    title_id = {}
    for file in glob.glob(input_file):

        with open(file, "r") as file:

            for line in file:
                line = json.loads(line)
                #title_id[line['title']] = {'id': line['id'], 'lc':line['lc'], 'wd': line['wd']}
                title_id[line['title']] = {'id': int(line['id']), 'lc':line['lc']}

    with open(output_file, "w") as file:
        json.dump(title_id, file)

    print("File converted")
