from postgres_db.matches.partnumbers_getter import get_partnumbers_by_type_input
from query_maker import query_maker_weaviate
from weaviate_codes.weaviate_query import find_matches_by_specs
from matches_registrar import insert_partnumber_data
from typing import List
def process_matches(type_input:str, type_output:List[str], specs_to_be_matched:str):
    rows = get_partnumbers_by_type_input(type_input)
    

    for row in rows:
        all_matches_for_a_partnumber = [{row['partnumber'] : row['brand']}]
        specs = row["specs"]
        query = query_maker_weaviate(specs, specs_to_be_matched)
        results = find_matches_by_specs(query, type_output)

        for res in results:
            match = {
                    res['Brand']:res['partNumber']}
            all_matches_for_a_partnumber.append(match)

    insert_partnumber_data(all_matches_for_a_partnumber)

# Example usage
if __name__ == "__main__":
    process_matches("SFJ", "SFJ")
