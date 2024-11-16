"""
Created on 2024-08-15

@author: wf
"""
import i18n
from mogwai.web.i18n_config import I18nConfig
from ngwidgets.login import Login
from ngwidgets.users import Users
from ngwidgets.input_webserver import InputWebserver, InputWebSolution
from ngwidgets.webserver import WebserverConfig
from mogwai.version import Version
from nicegui import Client, ui, run
from mogwai.web.node_view import  NodeTableView, NodeView, NodeViewConfig
from mogwai.core import MogwaiGraph, Traversal
from mogwai.examples.schema import MogwaiExampleSchema
from mogwai.schema.graph_schema import GraphSchema
from mogwai.graphs import Graphs
from mogwai.parser import PDFGraph, powerpoint_converter
from mogwai.parser.graphml_converter import graphml_to_mogwaigraph
from mogwai.parser.excel_converter import EXCELGraph
import mogwai.core.traversal as Trav
import os
import tempfile
from starlette.responses import RedirectResponse
from lodstorage.persistent_log import Log

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
        self.examples=Graphs()

        # the graph for displaying nodes
        self.graph = MogwaiGraph()
        yaml_path=MogwaiExampleSchema.get_yaml_path()
        self.schema = GraphSchema.load(yaml_path=yaml_path)
        self.schema.add_to_graph(self.graph)

        @ui.page("/")
        async def home(client: Client):
            return await self.page(client, MogwaiSolution.home)

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

        @ui.page("/nodes/{node_type}")
        async def show_nodes(client: Client, node_type: str):
            """
            show the nodes of the given type
            """
            await self.page(client, MogwaiSolution.show_nodes, node_type)

        @ui.page("/node/{node_type}/{node_id}")
        async def node(client: Client, node_type: str, node_id: str):
            """
            show the node with the given node_id
            """
            await self.page(client, MogwaiSolution.show_node, node_type, node_id)


    def configure_run(self):
        """
        configure with args
        """
        I18nConfig.config()

        InputWebserver.configure_run(self)

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
        self.log=Log()
        self.examples=webserver.examples
        self.graph = webserver.graph
        self.schema = webserver.schema
        self.graph_label=None
        self.result_html=None
        self.update_graph("modern")

    def authenticated(self) -> bool:
        """
        Check if the user is authenticated.
        Returns:
            True if the user is authenticated, False otherwise.
        """
        return self.webserver.login.authenticated()

    def configure_menu(self):
        """
        configure additional non-standard menu entries
        """
        with self.header:
            if self.authenticated():
                self.link_button("logout", "/logout", "logout", new_tab=False)
            else:
                self.link_button("login", "/login", "login", new_tab=False)
        # Sorting the node types by display_order
        sorted_node_types = sorted(
            self.schema.node_type_configs.items(),
            key=lambda item: item[1].display_order,
        )

        for node_type_name, node_type in sorted_node_types:  # label e.g. project_list
            label_i18nkey = f"{node_type.label.lower()}_list"
            label = i18n.t(label_i18nkey)
            path = f"/nodes/{node_type_name}"
            self.link_button(label, path, node_type.icon, new_tab=False)


    async def login_ui(self):
        """
        login ui
        """
        await self.webserver.login.login(self)

    async def home(self):
        """Provide the main content page"""
        await self.query_graph()

    async def show_nodes(self, node_type: str):
        """
        show nodes of the given type

        Args:
            node_type(str): the type of nodes to show
        """

        def show():
            try:
                config = NodeViewConfig(
                    solution=self,
                    graph=self.graph,
                    schema=self.schema,
                    node_type=node_type,
                )
                if not config.node_type_config:
                    ui.label(f"{i18n.t('invalid_node_type')}: {node_type}")
                    return
                node_table_view = NodeTableView(config=config)
                node_table_view.setup_ui()
            except Exception as ex:
                self.handle_exception(ex)

        await self.setup_content_div(show)

    async def show_node(self, node_type: str, node_id: str):
        """
        show the given node
        """

        def show():
            config = NodeViewConfig(
                solution=self, graph=self.graph, schema=self.schema, node_type=node_type
            )
            if not config.node_type_config:
                ui.label(f"{i18n.t('invalid_node_type')}: {node_type}")
                return
            # default view is the general NodeView
            view_class = NodeView
            # unless there is a specialization configured
            if config.node_type_config._viewclass:
                view_class = config.node_type_config._viewclass
            node_view = view_class(config=config, node_id=node_id)
            node_view.setup_ui()
            pass

        await self.setup_content_div(show)

    async def on_graph_select(self,vce_args):
        await run.io_bound(self.update_graph,vce_args.value)

    def update_graph(self,graph_name:str):
        try:
            self.graph_name=graph_name
            self.graph = self.load_graph(name=graph_name)
            self.get_graph_label()
            if self.graph_label:
                self.graph_label.update()
        except Exception as ex:
            self.handle_exception(ex)

    def get_graph_label(self)->str:
        self.graph_label_text=f"Query Graph {self.graph.name} {len(self.graph.nodes)} nodes {len(self.graph.edges)} edges"
        return self.graph_label_text

    async def query_graph(self):
        """Graph querying page"""
        def setup_query():
            emphasize="text-h5"
            try:
                with ui.row() as self.header_row:
                    graph_selection=self.examples.get_names()
                    self.graph_selector=self.add_select(
                        "graph",
                        graph_selection,
                        value=self.graph_name,
                        on_change=self.on_graph_select)
                if self.authenticated():
                    with ui.row() as self.upload_row:
                        ui.label("import File").classes(emphasize)
                        file_upload = ui.upload(label="Choose a file", multiple=False, auto_upload=True)
                        file_upload.on('upload', self.handle_upload)

                if self.graph:
                    self.get_graph_label()
                    self.graph_label=ui.label().classes(emphasize)
                    self.graph_label.bind_text_from(self, 'graph_label_text')
                    self.query_text_area = (
                        ui.textarea("Enter Gremlin Query")
                        .props("clearable")
                        .props("rows=5;cols=80")
                        .bind_value_to(self, "query")
                    )
                    ui.button("Run Query", on_click=lambda: self.on_run_query())
                else:
                    ui.label("No graph loaded. Please select a graph first.")
                with ui.row() as self.result_row:
                    self.result_html=ui.html()
            except Exception as ex:
                self.handle_exception(ex)

        await self.setup_content_div(setup_query)

    def load_graph(self,file=None,name:str="modern"):
        if file is None:
            if name in self.examples.get_names():
                graph=self.examples.get(name)
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

    def on_run_query(self, query:str=None):
        """Run a Gremlin query on the graph"""
        if not self.graph:
            ui.notify("No graph loaded. Please select a graph first.", type="warning")
            return
        try:
            if query is None:
                query=self.query
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
                count=len(query_result.result)
                plural_postfix="s" if count>1 else ""
                markup=f"{count} element{plural_postfix}:<br>"
                for i,traverser in enumerate(query_result.result):
                    markup+=f"{i+1}:{str(traverser)}<br>"
                self.result_html.content=markup

