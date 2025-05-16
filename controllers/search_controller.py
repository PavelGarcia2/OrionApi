from flask import Blueprint, request, jsonify
from services import search_service as ss
search_blueprint = Blueprint("search", __name__)

@search_blueprint.route("/", methods=["GET"])
def search():
    """
    Search for web pages matching the given query.
    ---
    parameters:
      - name: q
        in: query
        type: string
        required: true
        description: The search query string
    responses:
      200:
        description: A list of matching results
        examples:
          application/json: {
            "query": "best hostels in mallorca",
            "results": [
              {
                "url": "https://example.com/hostel1",
                "title": "Best Hostels in Mallorca – Top 10 Picks",
                "summary": "Discover the best affordable and scenic hostels on the island of Mallorca.",
                "score": 0.92
              }
            ]
          }
      400:
        description: No query provided
    """
    query = request.args.get("q")
    if not query:
        return jsonify({"error": "No query provided"}), 400

    response = ss.search_query(query)
    # response = {
    #     "query": query,
    #     "results": [
    #         {
    #             "url": "https://example.com/hostel1",
    #             "title": "Best Hostels in Mallorca – Top 10 Picks",
    #             "summary": "Discover the best affordable and scenic hostels on the island of Mallorca.",
    #             "score": 0.92
    #         },
    #         {
    #             "url": "https://example.com/hostel2",
    #             "title": "Mallorca Hostel Guide 2024",
    #             "summary": "Explore top-rated accommodations for backpackers and travelers in Mallorca.",
    #             "score": 0.88
    #         }
    #     ]
    # }

    return jsonify(response), 200
