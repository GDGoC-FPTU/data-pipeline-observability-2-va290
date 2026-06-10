import pandas as pd

def simulate_agent_response(query, data_path):
    """
    A simple "RAG-like" simulation. 
    It looks for the best match in the data and returns a response.
    """
    try:
        df = pd.read_csv(data_path)
        
        # Simple Logic: Look for the product with highest price or matching category
        if "electronic" in query.lower():
            subset = df[df['category'].str.lower() == 'electronics']
            if not subset.empty:
                best_deal = subset.loc[subset['price'].idxmax()]
                return f"Agent: Based on my data, the best choice is {best_deal['product']} at ${best_deal['price']}."
            else:
                return "Agent: Sorry, I don't see any electronics in my current knowledge base."
        
        return "Agent: I'm not sure how to answer that with the current data."
        
    except Exception as e:
        return f"Agent Error: I'm choking on the data! ({str(e)})"

if __name__ == "__main__":
    # Test with Clean Data
    print("Testing with CLEAN data:")
    print(simulate_agent_response("What is the best electronic product?", "processed_data.csv"))
    
    # Test with Garbage Data (to be created by students)
    print("\nTesting with GARBAGE data:")
    print(simulate_agent_response("What is the best electronic product?", "garbage_data.csv"))
