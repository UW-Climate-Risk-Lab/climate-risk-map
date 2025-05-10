import hashlib
import json

def generate_robust_hash(*args):
    """
    Generates a SHA256 hash for a combination of arguments including strings,
    dictionaries (including complex nested ones like GeoJSON), lists,
    numbers, and booleans.

    The hash is designed to be consistent:
    - The order of arguments passed to the function does not affect the hash.
    - The order of keys in dictionaries (at any level of nesting) does not
      affect the hash.
    - The order of elements in lists *does* affect the hash. If a list
      (e.g., a list of tags or selected items) should be order-insensitive
      for hashing purposes, sort that list *before* passing it as an
      argument to this function.

    Args:
        *args: Variable length arguments. Can include strings, dictionaries,
               lists, integers, floats, and booleans.

    Returns:
        str: A hexadecimal SHA256 hash string.
    """
    hasher = hashlib.sha256()

    # Convert all arguments to a canonical string representation with a type prefix.
    # This allows sorting them reliably to make the overall argument order irrelevant
    # and prevents type-based collisions (e.g., string "true" vs. boolean True).
    representations = []
    for arg in args:
        if isinstance(arg, str):
            # Prefix 's:' for string
            representations.append(f"s:{arg}")
        elif isinstance(arg, dict):
            # For dictionaries (including complex nested GeoJSON):
            # json.dumps with sort_keys=True creates a canonical string.
            # sort_keys=True ensures that the order of keys in the dictionary
            # (and any nested dictionaries) does not affect the resulting string.
            # separators=(',',':') creates the most compact, consistent JSON string.
            # This works perfectly for GeoJSON as it's a dictionary structure.
            dict_str = json.dumps(arg, sort_keys=True, separators=(',', ':'))
            representations.append(f"d:{dict_str}")
        elif isinstance(arg, list):
            # For lists (e.g., a list of strings, or coordinate arrays in GeoJSON):
            # json.dumps preserves the order of elements, which is typically
            # significant. If the order within a specific list input (like a
            # list of selected tags) should NOT matter, sort that list *before*
            # passing it to this function. E.g., generate_robust_hash(sorted_list_of_tags).
            # sort_keys=True here applies to any dictionaries *within* the list items.
            list_str = json.dumps(arg, sort_keys=True, separators=(',', ':'))
            representations.append(f"l:{list_str}")
        elif isinstance(arg, (int, float)):
            # For numbers, use json.dumps for a consistent string representation.
            representations.append(f"n:{json.dumps(arg)}")
        elif isinstance(arg, bool):
            # For booleans, json.dumps reliably produces 'true' or 'false'.
            representations.append(f"b:{json.dumps(arg)}")
        elif arg is None:
            # Consistent representation for None.
            representations.append("x:null") # json.dumps(None) results in 'null'
        else:
            # Fallback for other types. The str() representation might not always
            # be canonical or unique for custom objects. Consider if such types
            # will be passed or if they need specific handling or should raise an error.
            representations.append(f"u:{str(arg)}")

    # Sort the canonical string representations.
    # This ensures that generate_robust_hash("a", {"b":1}) produces the
    # same hash as generate_robust_hash({"b":1}, "a").
    representations.sort()

    # Update the hash with each sorted, prefixed, string representation.
    # Adding a separator between items is a good practice for clarity,
    # though the prefixes already largely prevent simple concatenation ambiguity.
    for rep in representations:
        hasher.update(rep.encode('utf-8'))
        hasher.update(b"|") # Separator

    return hasher.hexdigest()
