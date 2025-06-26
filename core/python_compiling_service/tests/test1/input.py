class UDFCode:
    def process_tuple(self, tuple_: dict, port: int):
        # Initialize some variables
        score = 0
        category = "unknown"
        
        # Process based on age
        if tuple_['age'] > 18:
            score += 10
            category = "adult"
            yield tuple_, 0  # Send to port 0 for adults
        else:
            score += 5
            category = "minor"
            yield tuple_, 1  # Send to port 1 for minors
            
        # Additional processing based on score
        if score > 8:
            tuple_['status'] = "high"
            yield tuple_, 2  # Send to port 2 for high scores
        else:
            tuple_['status'] = "low"
            yield tuple_, 3  # Send to port 3 for low scores
            
        # Final processing
        tuple_['category'] = category
        tuple_['score'] = score
        yield tuple_, 4  # Send to port 4 for final results 