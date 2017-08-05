import sys
sys.path.append("../")

from Instance_Subclass_collector import Instance_subclass_collector
from Subclass_graph_builder import Subclass_graph_builder
if __name__ == '__main__':
    collector = Instance_subclass_collector()
    collector.collect_instance_and_sublass()

    #subclass_graph_builder = Subclass_graph_builder()
    #subclass_graph_builder.build_graph()