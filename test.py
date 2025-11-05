# line1 = [1,2,3,4,5,6,7,8,9,10]
#  line2 = [2,4,6,8,10]
#  line3 = [4,8]
#  line4 = [8]

def get_no_of_counter(num): 
    no_of_people = num
    current_line = list(range(1, no_of_people + 1))
    lines = [current_line]
    while len(current_line) > 1:
        # Select every second person
        current_line = current_line[1::2]
        lines.append(current_line)
    return len(lines)-1

# Example usage:
result = get_no_of_counter(10)
print(result)
