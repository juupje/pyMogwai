"""
Created on 2024-08-15

@author: wf
"""
from ngwidgets.login import Login
from ngwidgets.users import Users
from ngwidgets.input_webserver import InputWebserver, InputWebSolution
from ngwidgets.webserver import WebserverConfig
from mogwai.version import Version
from nicegui import Client, ui, run
from mogwai.core import MogwaiGraph, Traversal
from mogwai.parser import PDFGraph, powerpoint_converter
from mogwai.parser.graphml_converter import graphml_to_mogwaigraph
from mogwai.parser.excel_converter import EXCELGraph
import mogwai.core.traversal as Trav
import os
import tempfile
from starlette.responses import RedirectResponse

class QueryResult:
    def __init__(self,traversal,result):
        self.traversal=traversal
        self.result=result

class MogwaiWebServer(InputWebserver):
    """
    Mogwai WebServer
    """
    @classmethod
    def get_config(cls) -> WebserverConfig:
        copy_right = "(c)2024 Wolfgang Fahl"
        config = WebserverConfig(
            copy_right=copy_right,
            version=Version(),
            default_port=9850,
            short_name="mogwai",
        )
        server_config = WebserverConfig.get(config)
        server_config.solution_class = MogwaiSolution
        return server_config

    def __init__(self):
        """Constructs all the necessary attributes for the WebServer object."""
        InputWebserver.__init__(self, config=MogwaiWebServer.get_config())
        users = Users("~/.solutions/mogwai")
        self.login = Login(self, users)

        @ui.page("/")
        async def home(client: Client):
            return await self.page(client, MogwaiSolution.home)

        @ui.page("/parse")
        async def parse_file(client: Client):
            if not self.login.authenticated():
                return RedirectResponse("/login")
            return await self.page(client, MogwaiSolution.parse_file)

        @ui.page("/query")
        async def query_graph(client: Client):
            return await self.page(client, MogwaiSolution.query_graph)

        @ui.page("/login")
        async def login(client: Client):
            return await self.page(client, MogwaiSolution.login_ui)

        @ui.page("/logout")
        async def logout(client: Client) -> RedirectResponse:
            if self.login.authenticated():
                await self.login.logout()
            return RedirectResponse("/")

    def authenticated(self) -> bool:
        """
        Check if the user is authenticated.
        Returns:
            True if the user is authenticated, False otherwise.
        """
        return self.login.authenticated()

class MogwaiSolution(InputWebSolution):
    """
    the Mogwai solution
    """

    def __init__(self, webserver: MogwaiWebServer, client: Client):
        """
        Initialize the solution

        Args:
            webserver (MogwaiWebServer): The webserver instance associated with this context.
            client (Client): The client instance this context is associated with.
        """
        super().__init__(webserver, client)
        self.graph=None
        self.graph_label=None
        self.result_html=None
        self.update_graph("modern")

    def setup_menu(self, detailed: bool = True):
        """
        setup the menu
        """
        super().setup_menu(detailed=detailed)
        ui.button(icon="menu", on_click=lambda: self.header.toggle())
        with self.header:
            if self.webserver.authenticated():
                self.link_button("logout", "/logout", "logout", new_tab=False)
            else:
                self.link_button("login", "/login", "login", new_tab=False)

    async def login_ui(self):
        """
        login ui
        """
        await self.webserver.login.login(self)

    async def home(self):
        """Provide the main content page"""
        await self.query_graph()

    async def parse_file(self):
        """File parsing page"""
        def setup_parse():
            ui.label("Parse File").classes('text-h4')
            file_upload = ui.upload(label="Choose a file", multiple=False, auto_upload=True)
            file_upload.on('upload', self.handle_upload)

        await self.setup_content_div(setup_parse)

    async def on_graph_select(self,vce_args):
        await run.io_bound(self.update_graph,vce_args.value)

    def update_graph(self,graph_name:str):
        self.graph_name=graph_name
        self.graph = self.load_graph(name=graph_name)
        if self.graph_label:
            with self.header_row:
                self.graph_label.content=self.get_graph_label()
                self.graph_label.update()

    def get_graph_label(self)->str:
        graph_label=f"Query {self.graph.name} graph"
        return graph_label

    async def query_graph(self):
        """Graph querying page"""
        def setup_query():
            try:
                with ui.row() as self.header_row:
                    graph_selection=["modern","crew"]
                    self.graph_selector=self.add_select(
                        "graph",
                        graph_selection,
                        value=self.graph_name,
                        on_change=self.on_graph_select)
                if self.graph:
                    self.graph_label=ui.label(self.get_graph_label()).classes('text-h5')
                    query = ui.input(label="Enter Gremlin query")
                    ui.button("Run Query", on_click=lambda: self.on_run_query(query.value))
                else:
                    ui.label("No graph loaded. Please select a graph first.")
                with ui.row() as self.result_row:
                    self.result_html=ui.html()
            except Exception as ex:
                self.handle_exception(ex)

        await self.setup_content_div(setup_query)

    def load_graph(self,file=None,name:str="modern"):
        if file is None:
            if name=="modern":
                graph=MogwaiGraph.modern()
            elif name=="crew":
                graph=MogwaiGraph.crew()
            else:
                raise ValueError(f"invalid graph name {name}")
            graph.name=name
        else:
            if file.name.endswith('.graphml'):
                temp_path = os.path.join(tempfile.gettempdir(), file.name)
                with open(temp_path, 'wb') as f:
                    f.write(file.read())
                graph = graphml_to_mogwaigraph(file=temp_path)
            elif file.name.endswith('.xlsx'):
                graph = EXCELGraph(file)
            elif file.name.endswith('.pdf'):
                graph = PDFGraph(file)
            elif file.name.endswith('.pptx'):
                graph = powerpoint_converter.PPGraph(file=file)
            else:
                raise ValueError(f"invalid file {file.name}")
            graph.name=file.name
        return graph

    def handle_upload(self, e):
        """Handle file upload"""
        file = e.content
        try:
            self.graph=self.load_graph(file)
        except Exception as ex:
            ui.notify(f"Unsupported file: {file.name} {str(ex)}", type="negative")
            return

        if self.graph:
            ui.notify("File parsed successfully", type="positive")
            ui.label(f"Imported a graph with {len(self.graph.nodes)} nodes and {len(self.graph.edges)} edges.")

    def on_run_query(self, query:str):
        """Run a Gremlin query on the graph"""
        if not self.graph:
            ui.notify("No graph loaded. Please select a graph first.", type="warning")
            return
        try:
            query_result=self.run_query(query)
            self.display_result(query_result)
        except Exception as e:
            ui.notify(f"Error executing query: {str(e)}", type="negative")

    def run_query(self,query)->QueryResult:
        g = Trav.MogwaiGraphTraversalSource(self.graph)
        traversal = eval(query, {'g': g})
        if not traversal.terminated:
            traversal=traversal.to_list()
        result = traversal.run()
        qr=QueryResult(traversal=traversal,result=result)
        return qr

    def display_result(self,query_result:QueryResult):
        if self.result_html:
            with self.result_row:
                markup=f"{len(query_result.result)} elements:<br>"
                for i,traverser in enumerate(query_result.result):
                    markup+=f"{i+1}:{str(traverser)}<br>"
                self.result_html.content=markup

