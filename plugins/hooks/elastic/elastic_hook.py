from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
from elasticsearch import Elasticsearch


class ElasticHook(BaseHook):
    def __init__(self, conn_id="elastic_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id)

        conn_config = {}
        hosts = []

        if conn.host:
            hosts = conn.host.split(",")
        if conn.port:
            conn_config["port"] = int(conn.port)
        if conn.login:
            conn_config["http_auth"] = (conn.login, conn.password)

        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema

    def info(self):
        return self.es.info()

    def set_index(self, index):
        self.index = index

    def add_doc(self, index, id, doc):
        self.set_index(index)
        res = self.es.index(index=index, id=id, body=doc)
        return res

    def get_doc(self, index, doc_id):
        res = self.es.get(index=index, id=doc_id)
        return res


class AirflowElasticPlugin(AirflowPlugin):
    name = "elastic_plugin"
    hooks = [ElasticHook]
