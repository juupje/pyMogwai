import os
import time
import argparse
import numpy as np
from mogwai.core.traversal import Traversal, MogwaiGraphTraversalSource
from gremlin_python.process.graph_traversal import GraphTraversalSource

class Time():
    def __init__(self):
        self.min = 0
        self.mean = 0
        self.max = 0
        self.times = 0
        
    def print(self):
        print("Timing results:")
        print(f"\tMin: {self.min:.3g}")
        print(f"\tMean: {self.mean:.3g}")
        print(f"\tMax: {self.max:.3g}")
        print(f"\tstd: {self.times:.3g}")

# ===== QUERY 1 =====

def query_mogwai_Lax(g:MogwaiGraphTraversalSource):
    from mogwai.core.steps.statics import outE, select, lte
    res = g.V().has_label('airport').has_name('LAX').as_('start').repeat(
            outE('route').as_('e').inV().filter_(
                select('e').properties('dist').is_(lte(2000))
            ).simple_path()
        ).times(3).emit().properties("city").dedup().to_list().run()
    assert len(res)==801

def query_grempy_Lax(g:GraphTraversalSource):
    from gremlin_python.process.graph_traversal import __
    from gremlin_python.process.traversal import P
    res = g.V().has_label('airport').has("code",'LAX').as_('start').repeat(
            __.outE('route').as_('e').inV().filter_(
                __.select('e').values('dist').is_(P.lte(2000))
            ).simple_path()
        ).times(3).emit().values('city').dedup().to_list()
    assert len(res)==801
    

# ===== QUERY 2 =====
def query_mogwai_furthest(g:MogwaiGraphTraversalSource):
    from mogwai.core.steps.statics import select
    from gremlin_python.process.graph_traversal import __
    res = g.V().has_label("airport").within("city", ["Brussels", "Maastricht", "Aachen", "Dusseldorf"]).as_('start')\
        .outE("route").as_("e").inV().in_("contains").has_label("country").as_('dest')\
        .order(desc=True).by(select('e').properties('dist'))\
        .limit(5).select('start', 'dest').by('desc').to_list()
    assert len(res)==5

def query_grempy_furthest(g:GraphTraversalSource):
    from gremlin_python.process.graph_traversal import __
    from gremlin_python.process.traversal import Order
    from gremlin_python.process.traversal import P
    res = g.V().has_label("airport").has("city", P.within(["Brussels", "Maastricht", "Aachen", "Dusseldorf"])).as_('start')\
            .outE("route").as_("e").inV().in_("contains").has_label("country").as_('dest')\
            .order().by(__.select('e').values('dist'),Order.desc)\
            .limit(5).select('start', 'dest').by("desc").to_list()
    assert len(res)==5

# ===== QUERY 3 =====

def query_mogwai_maastricht(g:MogwaiGraphTraversalSource):
    from mogwai.core.steps.statics import outE, select, lte, count, Scope
    res = g.V().has_label('airport').has_property("city", "Maastricht").as_('start').repeat(
            outE('route').as_('e').inV().filter_(
                select('e').properties('dist').is_(lte(2000))
            ).simple_path()
        ).times(3).emit().as_('dest')\
        .path().as_('p').count(Scope.local).order(asc=True).as_('length')\
        .select('dest').by('city').dedup().select('dest', 'length').to_list().by('city')
    assert len(res)==880

def query_grempy_maastricht(g:GraphTraversalSource):
    from gremlin_python.process.graph_traversal import __
    from gremlin_python.process.traversal import Order
    from gremlin_python.process.traversal import P
    from gremlin_python.process.traversal import Scope
    res = g.V().has_label('airport').has("city", "Maastricht").as_('start').repeat(
				__.outE('route').as_('e').inV().filter_(__.select('e').values('dist').is_(P.lte(2000))
			).simple_path()
        ).times(3).emit().as_('dest')\
        .path().as_('p').count(Scope.local).order().by(Order.asc).as_('length')\
        .select("dest").by("city").dedup().as_("dest").select('dest', 'length').to_list()
    assert len(res)==880

# ===== Query 4 =====
def query_mogwai_leave_germany(g:MogwaiGraphTraversalSource):
    res = g.V().has_label("country").has_property("desc", "Germany").out("contains").outE("route").as_("e").inV().in_("contains").has_label("country").as_('dest').select('e').order(desc=True).by("dist").limit(1).select('dest').by('desc').next().run()
    assert res=="Argentina"

def query_grempy_leave_germany(g:MogwaiGraphTraversalSource):
    from gremlin_python.process.traversal import Order
    res = g.V().has_label("country").has("desc", "Germany").out("contains").outE("route").as_("e").inV().in_("contains").has_label("country").as_('dest').select('e').order().by("dist",Order.desc).limit(1).select("dest").by("desc").next()
    assert res=="Argentina"

parser = argparse.ArgumentParser()
parser.add_argument("-n", help="Number of repetitions", required=True, type=int)
parser.add_argument("-q", "--query", help="Queries to run", nargs="+", default=[1,2,3,4], type=int)
args = vars(parser.parse_args())
air_routes_path=os.path.abspath("tests/documents/air-routes-latest.graphml")

def run_grempy_query(grempyquery):
    from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
    from gremlin_python.structure.graph import Graph
    from gremlin_python.driver.aiohttp.transport import AiohttpTransport
    from gremlin_python.process.traversal import P, T
    from gremlin_python.process.graph_traversal import __
    # SERVER STARTEN
    import subprocess
    times = []

    gremlin_server_path = '/home/joep/programs/gremlin-server'
    gremlin_command = f'{gremlin_server_path}/bin/gremlin-server.sh {gremlin_server_path}/conf/gremlin-server.yaml'
    for i in range(args["n"]):
        gremlin_server_process = subprocess.Popen(gremlin_command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        graph_gp = Graph()
        g_gp = graph_gp.traversal().with_remote(DriverRemoteConnection("ws://127.0.0.1:8182/gremlin", "g", transport_factory=lambda:AiohttpTransport(call_from_event_loop=True)))

        # read the content from the air routes example
        g_gp.V().drop().iterate()
        g_gp.io(air_routes_path).read().iterate()
        v_count=g_gp.V().count().next()

        tic = time.time()
        grempyquery(g_gp)
        toc = time.time()
        times.append(toc-tic)

        gremlin_server_process.kill()

    ret = Time()
    ret.min=min(times)
    ret.mean=sum(times)/len(times)
    ret.max=max(times)
    ret.times=np.std(times)
    return ret

def run_mogwai_query(mogwaiquery):
    from mogwai.parser import graphml_to_mogwaigraph
    from mogwai.core.traversal import MogwaiGraphTraversalSource

    times = []
    
    for i in range(args["n"]):
        graph_mogwai = graphml_to_mogwaigraph(air_routes_path, node_label_key="labelV", edge_label_key="labelE", node_name_key="code")
        g_m = MogwaiGraphTraversalSource(graph_mogwai)

        tic = time.time()
        mogwaiquery(g_m)
        toc = time.time()
        times.append(toc-tic)
    
    ret = Time()
    ret.min=min(times)
    ret.mean=sum(times)/len(times)
    ret.max=max(times)
    ret.times=np.std(times)
    return ret

def run_query(mogwai_query,grempy_query):
    print(mogwai_query.__name__)
    run_mogwai_query(mogwai_query).print()
    print(grempy_query.__name__)
    run_grempy_query(grempy_query).print()

queries = [(query_mogwai_furthest, query_grempy_furthest),
           (query_mogwai_Lax, query_grempy_Lax),
           (query_mogwai_maastricht,query_grempy_maastricht),
           (query_mogwai_leave_germany, query_grempy_leave_germany)]
for query_id in args['query']:
    print(f"Running query {query_id}")
    run_query(*queries[query_id-1])