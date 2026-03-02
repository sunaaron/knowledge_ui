#!/usr/bin/env python3
"""
Flask web service to display wxc_posts table data.
"""

from flask import Flask, render_template, jsonify, request
from mysql_utils import update_wxc_post_is_useful, delete_wxc_post, read_wxc_posts_by_category_and_latest_date, read_wxc_posts_by_category_and_date
import os

app = Flask(__name__)

def get_top_15_dates():
    """Get the top 15 most recent dates from the database."""
    connection = None
    try:
        from mysql_utils import create_connection
        connection = create_connection()
        if connection is None:
            return []
        
        cursor = connection.cursor(dictionary=True)
        
        # Query to get the top 15 most recent dates
        select_query = """
        SELECT DISTINCT date_str FROM wxc_posts 
        ORDER BY date_str DESC 
        LIMIT 15
        """
        
        cursor.execute(select_query)
        results = cursor.fetchall()
        
        # Extract just the date_str values
        dates = [row['date_str'] for row in results]
        return dates
        
    except Exception as e:
        print(f"Error fetching dates: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/')
def index():
    """Main page to display wxc_posts table data."""
    # Get filter parameters from query string
    filter_param = request.args.get('filter', 'both')
    date_filter = request.args.get('date', '')
    category_filter = request.args.get('category', 'znjy')  # Default to 'znjy' (子女教育)
    
    # Get top 15 dates for the dropdown
    top_dates = get_top_15_dates()
    
    # Use the new function to get posts by latest date for a specific category
    if date_filter:
        # If a specific date is selected, get posts for that date and category
        posts = read_wxc_posts_by_category_and_date(category_filter, date_filter)
    else:
        # Otherwise, get posts for the latest date in the category
        posts = read_wxc_posts_by_category_and_latest_date(category_filter)
    
    # Prepare data for display - only show posts with non-empty LLM summaries
    display_data = []
    for post in posts:
        llm_summary = post.get('llm_summary', '')
        if llm_summary and llm_summary.strip():  # Check if LLM summary exists and is not empty
            # Apply filter logic
            is_useful = post.get('is_useful', 0) == 1
            
            # Filter posts based on the filter parameter
            if filter_param == 'both':
                # Show all posts
                show_post = True
            elif filter_param == 'useful_only':
                # Show only useful posts
                show_post = is_useful
            elif filter_param == 'useless_only':
                # Show only useless posts
                show_post = not is_useful
            else:
                # Default to showing all posts
                show_post = True
            
            if show_post:
                display_data.append({
                    'id': post.get('id', ''),
                    'post_url': post.get('post_url', ''),
                    'post_title': post.get('post_title', ''),
                    'post_body': post.get('post_body', '')[:200] + '...' if len(post.get('post_body', '')) > 200 else post.get('post_body', ''),
                    'llm_summary': llm_summary,
                    'is_useful': is_useful,
                    'num_comments': post.get('num_comments', 0)  # Add num_comments field
                })
    
    return render_template('index.html', posts=display_data, filter=filter_param, date_filter=date_filter, category_filter=category_filter, top_dates=top_dates)

@app.route('/update_is_useful/<int:post_id>/<int:value>')
def update_is_useful(post_id, value):
    """Endpoint to update the is_useful field of a post."""
    success = update_wxc_post_is_useful(post_id, bool(value))
    return jsonify({'success': success})

@app.route('/delete_post/<int:post_id>')
def delete_post(post_id):
    """Endpoint to delete a post from the database."""
    success = delete_wxc_post(post_id)
    return jsonify({'success': success})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)