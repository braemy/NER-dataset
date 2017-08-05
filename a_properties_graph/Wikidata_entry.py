import json


class Wikidata_entry(object):
    def __init__(self, content):
        self.entry = json.loads(content)
        self.instance_of_id = "P31"  # that class of which this subject is a particular example and member. (Subject typically an individual member with Proper Name label.)
        # Different from P279 (subclass of).
        self.subclass_of_id = "P279"  # all instances of these items are instances of those items; this item is a class (subset) of
        # that item. Not to be confused with Property:P31 (instance of).

    def getTitle(self):
        try:
            labels = self.entry['labels']
        except:
            return None, None
        id_ = self.entry['id']
        # try to extract english title:
        try:
            title = labels['en']['value']
            return id_, title
        except:
            if list(labels):
                title = labels.get(list(labels)[0])['value']
                return id_, title
        return None, None

    def extract_instance_subclass(self):
        claims = self.concat_claims(self.entry['claims'])
        instance_of_list = []
        subclass_of_list = []
        e1 = self.entry['id']
        try:
            for claim in claims:
                mainsnak = claim['mainsnak']
                # print(mainsnak)
                # print()
                if mainsnak['snaktype'] != "value":
                    continue
                # if mainsnak['datatype'] == 'wikibase-item':
                property_ = mainsnak['property']
                if property_ == self.instance_of_id:

                    try:
                        instance = mainsnak['datavalue']['value']['id']
                    except:
                        instance = "Q" + str(mainsnak['datavalue']['value']['numeric-id'])
                    instance_of_list.append(instance)
                if property_ == self.subclass_of_id:
                    try:
                        subclass = mainsnak['datavalue']['value']['id']
                    except:
                        subclass = "Q" + str(mainsnak['datavalue']['value']['numeric-id'])
                    subclass_of_list.append(subclass)
        except:
            instance_of_list, subclass_of_list
        return instance_of_list, subclass_of_list






    def concat_claims(self, claims):
        for rel_id, rel_claims in claims.items():
            for claim in rel_claims:
                yield claim