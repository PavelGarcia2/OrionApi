from flask import Flask
from flasgger import Swagger
from controllers.search_controller import search_blueprint

app = Flask(__name__)

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "apispec",
            "route": "/apispec.json",
            "rule_filter": lambda rule: True,  # include all endpoints
            "model_filter": lambda tag: True,  # include all models
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/swagger"  # this changes the default from /apidocs to /swagger
}

swagger = Swagger(app, config=swagger_config)
app.register_blueprint(search_blueprint, url_prefix="/search")

if __name__ == "__main__":
    app.run(debug=True)
