import json
import sys
import argparse
from datetime import datetime

def analyze_budget(category=None, month=None, quarter=None):
    """
    Analyze budget data based on optional filters
    """
    # Sample data - in a real scenario, this might come from a database or file
    budget_data = [
        {"category": "Marketing", "month": "January", "quarter": "Q1", "budget": 50000, "actual": 48500, "variance": -1500},
        {"category": "Marketing", "month": "February", "quarter": "Q1", "budget": 45000, "actual": 46200, "variance": 1200},
        {"category": "Marketing", "month": "March", "quarter": "Q1", "budget": 55000, "actual": 52800, "variance": -2200},
        {"category": "Sales", "month": "January", "quarter": "Q1", "budget": 75000, "actual": 78200, "variance": 3200},
        {"category": "Sales", "month": "February", "quarter": "Q1", "budget": 72000, "actual": 71500, "variance": -500},
        {"category": "Sales", "month": "March", "quarter": "Q1", "budget": 80000, "actual": 82100, "variance": 2100},
        {"category": "Operations", "month": "January", "quarter": "Q1", "budget": 120000, "actual": 118500, "variance": -1500},
        {"category": "Operations", "month": "February", "quarter": "Q1", "budget": 125000, "actual": 122000, "variance": -3000},
        {"category": "Operations", "month": "March", "quarter": "Q1", "budget": 128000, "actual": 129500, "variance": 1500},
        {"category": "Marketing", "month": "April", "quarter": "Q2", "budget": 60000, "actual": 58700, "variance": -1300},
        {"category": "Sales", "month": "April", "quarter": "Q2", "budget": 85000, "actual": 87200, "variance": 2200},
        {"category": "Operations", "month": "April", "quarter": "Q2", "budget": 130000, "actual": 131500, "variance": 1500}
    ]
    
    # Filter data based on parameters
    filtered_data = budget_data
    
    if category:
        filtered_data = [item for item in filtered_data if item["category"].lower() == category.lower()]
    
    if month:
        filtered_data = [item for item in filtered_data if item["month"].lower() == month.lower()]
    
    if quarter:
        filtered_data = [item for item in filtered_data if item["quarter"].lower() == quarter.lower()]
    
    # Calculate summary statistics
    if filtered_data:
        total_budget = sum(item["budget"] for item in filtered_data)
        total_actual = sum(item["actual"] for item in filtered_data)
        total_variance = sum(item["variance"] for item in filtered_data)
        variance_percent = (total_variance / total_budget) * 100 if total_budget > 0 else 0
        
        summary = {
            "total_budget": total_budget,
            "total_actual": total_actual,
            "total_variance": total_variance,
            "variance_percent": round(variance_percent, 2),
            "record_count": len(filtered_data),
            "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    else:
        summary = {
            "message": "No data found for the specified filters",
            "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    # Prepare output
    result = {
        "summary": summary,
        "data": filtered_data,
        "filters": {
            "category": category,
            "month": month,
            "quarter": quarter
        }
    }
    
    return result

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Budget Finance Analysis Tool')
    parser.add_argument('--category', type=str, help='Filter by category (Marketing, Sales, Operations)')
    parser.add_argument('--month', type=str, help='Filter by month (January, February, etc.)')
    parser.add_argument('--quarter', type=str, help='Filter by quarter (Q1, Q2, etc.)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run analysis
    result = analyze_budget(
        category=args.category,
        month=args.month,
        quarter=args.quarter
    )
    
    # Print JSON result (will be captured by the FastAPI script)
    print(json.dumps(result, indent=2))