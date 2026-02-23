#!/usr/bin/env python3
"""
MySQL reader to read from wxc_posts table in the remote MySQL instance.
"""

import mysql.connector
from mysql.connector import Error
from datetime import datetime

def create_connection():
    """Create connection to MySQL database."""
    try:
        connection = mysql.connector.connect(
            host='192.168.86.55',
            port=3306,
            database='wxc_crawler',
            user='crawler_admin',
            password='123'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def read_wxc_posts_by_category_and_date(category, date_str):
    """
    Read posts from wxc_posts table filtered by category and date_str.
    
    Args:
        category (str): The category to filter by
        date_str (str): The date string in mmddyyyy format to filter by
    
    Returns:
        list: List of posts matching the criteria, or empty list if none found
    """
    connection = create_connection()
    if connection is None:
        return []
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Query to select posts by category and date_str
        select_query = """
        SELECT * FROM wxc_posts 
        WHERE category = %s AND date_str = %s
        """
        
        cursor.execute(select_query, (category, date_str))
        results = cursor.fetchall()
        
        print(f"Found {len(results)} posts matching category '{category}' and date '{date_str}'")
        return results
        
    except Error as e:
        print(f"Error reading from wxc_posts table: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def read_wxc_posts_by_category_and_latest_date(category):
    """
    Read posts from wxc_posts table for the latest date in a given category.
    
    First fetches max(date_str) for the given category, then calls 
    read_wxc_posts_by_category_and_date with that date.
    
    Args:
        category (str): The category to filter by
    
    Returns:
        list: List of posts matching the latest date in the category, 
              or empty list if none found
    """
    connection = create_connection()
    if connection is None:
        return []
    
    try:
        cursor = connection.cursor(dictionary=True)
        
        # First query to get the maximum date_str for the given category
        max_date_query = """
        SELECT MAX(date_str) as latest_date FROM wxc_posts 
        WHERE category = %s
        """
        
        cursor.execute(max_date_query, (category,))
        result = cursor.fetchone()
        
        if result and result['latest_date']:
            latest_date_str = result['latest_date']
            
            # Now call the existing function with the latest date
            return read_wxc_posts_by_category_and_date(category, latest_date_str)
        else:
            print(f"No posts found for category '{category}'")
            return []
            
    except Error as e:
        print(f"Error reading from wxc_posts table: {e}")
        return []
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def update_wxc_post_is_useful(post_id, is_useful):
    """
    Update a post in wxc_posts table with the is_useful value.
    
    Args:
        post_id (int): The ID of the post to update
        is_useful (bool): The is_useful value to store (True/False)
    
    Returns:
        bool: True if update was successful, False otherwise
    """
    connection = create_connection()
    if connection is None:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Convert boolean to integer (0 or 1) for storage in MySQL
        is_useful_int = 1 if is_useful else 0
        
        # Query to update the post with is_useful value
        update_query = """
        UPDATE wxc_posts 
        SET is_useful = %s 
        WHERE id = %s
        """
        
        cursor.execute(update_query, (is_useful_int, post_id))
        connection.commit()
        
        if cursor.rowcount > 0:
            print(f"Successfully updated is_useful for post with ID {post_id}")
            return True
        else:
            print(f"No post found with ID {post_id} to update")
            return False
            
    except Error as e:
        print(f"Error updating wxc_posts table: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def delete_wxc_post(post_id):
    """
    Delete a post from wxc_posts table.
    
    Args:
        post_id (int): The ID of the post to delete
    
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    connection = create_connection()
    if connection is None:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Query to delete the post
        delete_query = """
        DELETE FROM wxc_posts 
        WHERE id = %s
        """
        
        cursor.execute(delete_query, (post_id,))
        connection.commit()
        
        if cursor.rowcount > 0:
            print(f"Successfully deleted post with ID {post_id}")
            return True
        else:
            print(f"No post found with ID {post_id} to delete")
            return False
            
    except Error as e:
        print(f"Error deleting from wxc_posts table: {e}")
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def test_database_connection():
    """Test if we can connect to the database."""
    connection = create_connection()
    if connection:
        print("Successfully connected to MySQL database")
        connection.close()
        return True
    else:
        print("Failed to connect to MySQL database")
        return False

# Example usage
if __name__ == "__main__":
    # Test the database connection
    print("Testing MySQL connection...")
    test_database_connection()
    
    # Example queries
    print("\n--- Reading posts by category and date ---")
    posts = read_wxc_posts_by_category_and_date("znjy", "02212025")
