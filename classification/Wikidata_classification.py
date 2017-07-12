import sys

#from graph_tool import *
#from graph_tool.search import *
from tqdm import tqdm

from NER_mapping import NER_mapping
from utils import *


class Wikidata_classification(object):
    def  __init__(self):
        self.parameter_wd = load_parameters('wikidata')
        self.parameter_gr = load_parameters()
        self.folder = self.parameter_wd['output_dir']
        self.id_to_instance = dict()

    def load_instance(self):
        self.id_to_instance = load_instance(self.folder)

    def load_subclass(self):
        self.id_to_instance = load_subclass(self.folder)

    def load_id_to_title(self):
        self.id_to_title = load_id_to_title(self.folder)

    def load_title_to_id(self):
        self.title_to_id = load_title_to_id(self.folder)

    def classify_article(self):
        self.load_instance()
        ner_mapping = NER_mapping()
        id_to_nerClass = dict()
        for id_, instances in tqdm(self.id_to_instance.items()):
            ner_class = ner_mapping.classify_entity_by_instances(instances)
            if ner_class:
                id_to_nerClass[id_] = ner_class

        pickle_data(id_to_nerClass, self.parameter_wd['wd_to_NER'])

        convert_wp_to_(self.parameter_wd['wd_to_NER'], "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
        print("Ratio of pages in more than 1 categories:",
              round(float(ner_mapping.fuck_counter) / (len(id_to_nerClass)), 10) * 100, "%")



























    def get_subset_of_wikidata(self, list_of_id, name):
        file_path = os.path.join("/dlabdata1/braemy/wikidata-classification/", name + ".p")
        # self.build_mapping(list_of_id, file_path)

        self.load_instance()
        wd_id_of_subclasses = load_pickle(file_path)
        wd_set_output = set()
        for id_, instances in tqdm(self.id_to_instance.items()):
            for instance in instances:
                if instance in wd_id_of_subclasses:
                    wd_set_output.add(id_)
        convert_wd_id_to_wp_title(wd_set_output,
                                  "/dlabdata1/braemy/wikipedia_classification/wp_by_title_" + name + ".p")

    def build_mapping_to_NER_class(self):
        self.load_id_to_title()

        id_to_vertex = load_pickle(self.parameter_gr['graph_id_to_vertex'])

        print("Loading graph...")
        graph = load_graph(self.parameter_gr['graph'])
        print("graph loaded")


        per = ['Q215627']  # ['person']
        org = ['Q43229']  # ['organization']
        loc = ['Q3257686', 'Q17334923']  # ['locality', 'location']
        misc = ['Q1656682', 'Q315', 'Q231002', 'Q1190554']  # ['event', 'language', 'nationality', 'event]
        self.build_mapping(per, self.parameter_wd['PER'], graph, id_to_vertex)
        self.build_mapping(loc, self.parameter_wd['LOC'], graph, id_to_vertex)
        self.build_mapping(org, self.parameter_wd['ORG'], graph, id_to_vertex)
        self.build_mapping(misc, self.parameter_wd['MISC'], graph, id_to_vertex)

    def build_mapping(self,list_of_id, file_path, graph=None, id_to_vertex=None ):
        if graph is None:
            graph = load_graph(self.parameter_gr['graph'])
            id_to_vertex = load_pickle(self.parameter_gr['graph_id_to_vertex'])
        vertex_properties = graph.vertex_properties['properties']
        print("Building mapping for: ")
        l = set()
        for id_ in list_of_id:
            print("  -", self.id_to_title[id_][0])
            v = graph.vertex(id_to_vertex[id_])
            v_iterator = bfs_iterator(graph, source=v)
            for o in v_iterator:
                l.add(vertex_properties[o.source()][0])
                l.add(vertex_properties[o.target()][0])
        with open(file_path, "wb") as file:
            pickle.dump(l, file)
        print(len(l), "elements in", file_path)
        return l