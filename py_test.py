import datetime
import math
import json
from itertools import permutations
from collections import Counter

def show_datetime():
    now = datetime.datetime.now()
    print(now.strftime("%Y-%m-%d %H:%M:%S"))

def area_of_circle (radius):
    print(2*math.pi*radius)

def create_list_and_tuple(values):
    lists = values.split(',')
    tuples = tuple(lists)

    print('list: ',lists)
    print('tuple: ',tuples)

def compute_numerics(value, multiplier):
    total = 0
    number = str(value)
    for i in range(1,multiplier+1):
        new_no = i*number
        total = total + int(new_no)
    print(total)

def print_doc_string():
    '''
        I am learning docstrings
    '''

def date_subtractor():
    f_date = datetime.date(2014,7,2)
    l_date = datetime.date(2014,7,11)

    diff = l_date - f_date
    print(diff.days)

def number_occurences(values):
    values = tuple(values)
    nineteen = 0
    five = 0
    for i in values:
        if (i==19):
            nineteen = nineteen+1
        elif(i==5):
            five = five+1
        if(nineteen==2 and five>=3):
            print("True")
            break  
    if(nineteen!=2): 
        print("False") 

def check_numbers(start,end):
    numbers=[]
    for i in range(start, end+1):
        if(i%7==0 and i%5==0):
            numbers.append(i)
    print(numbers)

def display_asterix(n):
    for i in range(n):
        for j in range(i):
            print("* ", end="")
        print('')
    
    for i in range(n,0,-1):
        for j in range(i,0,-1):
            print("* ", end="")
        print('')

def word_reverse(word):
    new_word = list(word)
    length = len(new_word)
    for i in range(length-1,-1,-1):
        print(new_word[i],end="")

def fibonacci():
    x, y= 0,1
    while y<50:
        print(y)
        x=y
        y=x+y

def print_json(json_string):
    python_obj = json.dumps(json_string)
    print(python_obj)

def list_duplicates():
    items_list=[10, 20, 30, 20, 10, 50, 60, 40, 80, 50, 40]
    unique_list=[]
    for i in items_list:
        if i not in unique_list:
            unique_list.append(i)
    print(unique_list)

def last(n):
    return n[-1]

def sort_list():
    input_list = [(2, 5), (1, 2), (4, 4), (2, 3), (2, 1)]
    return sorted(input_list,key=last)

def reverse_list(list, start, end):
    new_list = list[start:end+1]
    start = list[0:start]
    end = list[end+1:len(list)]
    #del list[start:end+1]
    new_list.reverse()
    start.extend(new_list)
    start.extend(end)
    print(start)

def longest_subsequence(list):
    n = len(list)
    if(n==1):
        print("invalid list")
        return
    
    max_value = 0
    counter = 0
    for i in range(0,n-1):
        if(list[i+1]>list[i]):
           counter += 1
        else:
            if(counter+1 > max_value):
                max_value = counter+1
            counter = 0
            i += 1
    
    if(counter+1 > max_value):
            max_value = counter+1
    print(max_value)

def print_list(list):
    if(len(list) == 0):
        return []
    if (len(list) == 1):
        return [list]
    
    l = []
    for i in range(len(list)):
        m = list[i]
        rem_list = list[:i] + list[i+1:]
        
        for p in print_list(rem_list):
            l.append([m]+p)

    print(l)

def get_sort_key(tuple):
    return tuple[1]

def dictionary_sort(dict):
    print(dict.items())
    sorted_dict = sorted(dict.items(), key=get_sort_key, reverse=True)
    print(sorted_dict)

def add_item(dict):
    dict.update({2:30})
    return dict

def iterate_dictionary(dict):
    for dict_key, dict_value in dict.items():
        print(dict_key,'->',dict_value)

def generate_dict(n):
    dictionary = {}
    for i in range(1,n+1):
        dictionary.update({i:i*i})
    return dictionary

def generate_sum(dict):
    return sum(dict.keys())

def collections_counter():
    shoes = 10
    sizes = [2,3,4,5,6,8,7,6,5,18]
    customers = 6
    desired_sizes = [(6,55),(6,45),(6,55),(4,40),(18,60),(10,50)]

    shoe_counter = 0
    earning = 0

    for items in desired_sizes:
        if(items[0] in sizes):
            earning += items[1]
            shoe_counter +=1
        
        if(shoe_counter>=shoes):
            break
    
    print(f'earning:{earning} shoe count:{shoe_counter}')

def sets_operation():
    int_array = []


    

    
#show_datetime()
#area_of_circle(2)
#create_list_and_tuple('1,2,3,4,5')
#compute_numerics(5,3)
#print(print_doc_string.__doc__)
#date_subtractor()
#number_occurences([19, 19, 15, 5, 3, 3, 5, 2, 6, 7])
#check_numbers(1500,2700)
#display_asterix(20)
#word_reverse('Ranith')
#fibonacci()
#print_json({"Name":"Ranith","Age":"34","Gender":"M"})
#list_duplicates()
#print(sort_list())
#reverse_list([10, 20, 30, 40, 50, 60, 70, 80],2,4)
#longest_subsequence([10,20,2,5,1])
#print_list([10,20,2])
#dictionary_sort({1: 2, 3: 4, 4: 3, 2: 1, 0: 0})
#print(add_item({0: 10, 1: 20}))
#iterate_dictionary({0: 10, 1: 20})
#print(generate_dict(5))
#print(generate_sum({1: 2, 3: 4, 4: 3, 2: 1, 0: 0}))
collections_counter()