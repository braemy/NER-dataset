import sys
sys.path.append("../")

from graph_tool import *
from graph_tool.search import *
from utils import *

from tqdm import tqdm
import pickle

class Subclass_graph_builder(object):
    def __init__(self):
        self.parameters = load_parameters()
        self.folder = self.parameters['wikidata']['output_dir']
        self.id_to_subclass = dict()
        self.load_id_to_title()


    def load_data(self):
        self.load_subclass()

    def load_subclass(self):
        self.id_to_subclass = load_subclass(self.folder)

    def load_id_to_title(self):
        self.id_to_title = load_id_to_title(self.folder)

    def build_graph(self):
        self.load_data()
        g = Graph()
        v_name = g.new_vertex_property("string")
        v_properties = g.new_vertex_property("object")
        id_to_v = dict()

        def add_vertex_helper(id_):
            if id_ not in id_to_v:
                v = g.add_vertex()
                title = self.id_to_title[id_][0] if id_ in self.id_to_title else "None"
                v_properties[v] = (id_, title)
                v_name[v] = (title)
                id_to_v[id_] = g.vertex_index[v]
                return v
            else:
                return id_to_v[id_]
        print("Total elements: ", len(self.id_to_subclass))
        for i, (id_, subclasses_of) in tqdm(enumerate(self.id_to_subclass.items())):
            v = add_vertex_helper(id_)
            for subclass_of in subclasses_of:
                v_parent = add_vertex_helper(subclass_of)
                edge = g.edge(v, v_parent)
                if not edge:
                    g.add_edge(v, v_parent)
                    # if i == 100:
                    # break
        g.vertex_properties['name'] = v_name
        g.vertex_properties['properties'] = v_properties
        g.save(self.parameters['graph'])
        pickle_data(id_to_v, self.parameters['graph_id_to_vertex'])
        return g, v_name, v_properties, id_to_v


