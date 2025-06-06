def process_type_input_notification(payload: dict):
    action = payload.get("action")

    if action == "batch_insert":
        # New batch notification format
        similar_types = payload.get("similar_types", "")
        specs_to_be_matched = payload.get("specs_to_be_matched", "")

        print("🔔 Batch Insert Notification")
        print(f"Similar Types: {similar_types}")
        print(f"Specs to be Matched: {specs_to_be_matched}")
        print("-" * 30)

        # Split once
        types_list = [t.strip() for t in similar_types.split(',') if t.strip()]
        if not types_list:
            return None, [], specs_to_be_matched

        return types_list[0], types_list[1:], specs_to_be_matched
        
    else:
        # Fallback for individual row notifications
        type_input = payload.get("type_input")
        type_output = payload.get("type_output")
        specs_to_be_matched = payload.get("Specs_to_be_Matched")

        print(f"Action: {action}")
        print(f"Type Input: {type_input}")
        print(f"Type Output: {type_output}")
        print(f"Specs to be matched: {specs_to_be_matched}")
        print("-" * 30)

        return type_input, type_output, specs_to_be_matched
