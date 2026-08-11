from admin_api.collector.plugins.plugin import DataSourcePlugin


class TelegrafVscode(DataSourcePlugin):
    plugin_id = "telegraf_vscode"

    def render_config(self, datasources: list):
        print("Rendering telegraf-vscode plugin")
        return "Rendered config"

    def reload(self) -> None:
        print("Reloading telegraf-vscode plugin")
        return None
