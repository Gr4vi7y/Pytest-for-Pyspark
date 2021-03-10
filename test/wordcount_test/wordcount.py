def do_word_counts(lines):
    counts = (lines.flatMap(lambda x: x.split())
                  .map(lambda x: (x, 1))
                  .reduceByKey(lambda x, y: x+y)
             ) 
    results = {word: count for word, count in counts.collect()}
    return results