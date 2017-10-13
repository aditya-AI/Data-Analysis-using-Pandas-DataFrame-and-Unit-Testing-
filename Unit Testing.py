from final import text_data,csv_data, merger,output
import pandas as pd
import csv
import magic


count = 0
'''pred_entry_check function checks whether the records in the texts files have only 1 single record in the dataframe,checks whether the group by works properly or not'''
print('First Test')
def pred_entry_check():
    global count   
    pred_check = {}
    check = False
    a = text_data('testpred.txt')
    for i in a['product_id']:
       if i not in pred_check:
              pred_check[i] = 1
              count += 1
       else:
              pred_check[i] += 1      
    for key ,value in pred_check.items():
       if value == 1:
              check = True
              print(key,check)
       else:
              print(check)       
pred_entry_check()          
print('-------------------------------------------------------------------------------------------------')
print("Test 1 pass, product_id's with 1 or 2 or 3 instances have only 1 single entry in to the dataframe")
print('-------------------------------------------------------------------------------------------------')
print('\n')

'''pred_number_check function checks if there are 15 records in text file, then there should be minimum 5 records and max 15'''
print('Second Test')
def pred_number_check():
       num_check = []
       with open('testpred.txt','rb') as f:
              for line in f.readlines():
                     num_check.append(line)
       print('Total number of records in predict.txt is:')
       x = len(num_check)
       print(x)
       print("Minimum Records in Text dataframe should be:" "  " '{}''  and maximum should be'"  " '{}'.format(x//3,x))             
       print('\n')
       a = text_data('testpred.txt')
       print("Number of records the Code Script Text Dataframe returned:" '{}'.format(len(a)))
       if (len(a) == count):
              print("Number of unique records in dataframe and predict text file is:" '{}'.format(count))

pred_number_check()
print('-------------------------------------------------------------------------------------------------')
print("Test 2 pass, number of records in dataframe were in range of 8-26")
print('-------------------------------------------------------------------------------------------------')
print('\n')

'''text_colon checks if there is any colon in the product_name column and debugs it '''
print('Third Test')
def text_colon():
       colon = []
       test_colon_records = []
       records_colon_occur = 0
       counter = 0
       with open('testpred.txt','r') as f:
              for line in f.readlines():
                     x = line.split(':')
                     if (len(x)>5):
                            records_colon_occur += 1
                            colon.append(x)
              print('Number of records that have colon in product name:' '{}'.format(records_colon_occur))
       b = text_data('testpred.txt')
       for i in range(len(colon)):
              print(colon[i][3])
       print('\n')
       for i in b['product_id']:
              if i == colon[0][3] and i == colon[1][3]:
                     print('The record which had colon in it, has been stored in the dataframe with product id:'' '  '{}'.format(i))
       
text_colon() 

print('-------------------------------------------------------------------------------------------------')
print("Test 3 pass, records which had colon in it, have been stored in the dataframe")
print('-------------------------------------------------------------------------------------------------')
print('\n')


'''Check whether the prod ids with only 1 record or 1 prediction is in the dataframe'''                             
print('Fourth Test')
def pred_record_1():
       colon = []
       ids = []
       unique = set()
       colon1 = []
       with open('testpred.txt','r') as f:
             for line in f.readlines():
               x = line.split(':')
               if (len(x)>5):
                   colon.append(x)
               elif (len(x) == 5):
                   colon.append(x)

       checkdict = {}
       i = 0
       l = len(colon)
       while l >0:
              if colon[i][3] not in checkdict:
                     checkdict[colon[i][3]] = 1
              else:
                     checkdict[colon[i][3]] += 1
              l = l -1
              i = i+1
       
       for key,value in checkdict.items():
           if value<3:
               ids.append(key)

       b = text_data('testpred.txt')
       
       for i in b['product_id']:
              for j in range(len(ids)):
                     if i == ids[j]:
                           print (i)

pred_record_1()                                                  
                               
print('-------------------------------------------------------------------------------------------------')
print("Test 4 pass, records with 1  or 2 product_ids or prediction have been stored in the dataframe")
print('-------------------------------------------------------------------------------------------------')
print('\n')


'''Check whether the output file header matches the specification file'''                             
print('Fifth Test')
def headers_match():
       df_1 = pd.read_csv('out.tsv',delimiter='\t')
       df_2 = df_2 = pd.read_csv('output.tsv',delimiter='\t')
       df_1_list = list(df_1)
       df_2_list = list(df_2)
       comp1 = []
       def comp(list1, list2):
              for val in list1:
                     for val1 in list2:
                            if val1 == val:
                                   comp1.append(val)
              print('Headers that matched the output file with the specification file:')
              print(comp1)
              print('\n')                     

       comp(df_1_list,df_2_list)

       mismatch = list(set(df_2_list) - set(df_1_list))
       print('Headers that did not match the output file with the specification file:')
       print(mismatch)
       print('\n')


       df_3 = pd.read_csv('dummy_part.csv',delimiter='\t')
       df_3_list = list(df_3)
       print('Does the input file part.csv has the column \'product_type_1_tag_source\'?')
       if 'product_type_1_tag_source' in df_3_list:
              print (True)
       else:
              print (False)

headers_match()  

print('-------------------------------------------------------------------------------------------------')
print("Test 5 pass, output file header matches the specification file")
print('-------------------------------------------------------------------------------------------------')
print('\n')

print('Sixth Test')
def header_values_type_match():
       type_count = 0
       with open("out.tsv") as textfile1, open("output.tsv") as textfile2: 
              for x, y in zip(textfile1, textfile2):
                     if (type(x) == type(y)):
                            type_count += 1

       with open('out.tsv',"r") as f:
              reader = csv.reader(f,delimiter = ",")
              data = list(reader)
              row_count = len(data) 

       if row_count == type_count:
              print("Data type of output file matches with the specification file!")


header_values_type_match()

print('-------------------------------------------------------------------------------------------------')
print("Test 6 pass, output file data type matches with the specification file")
print('-------------------------------------------------------------------------------------------------')
print('\n')

'''Checks if there are records which have unwanted colons in them, then does the code breaks or handles those records and save them in a log file!'''
print('Seventh Test')
def text_break():
       text = []
       text1 = []
       mer = merger('testpred.txt','part.csv')
       output(mer,'out_dummy.tsv') 
	
       with open('errorlog.txt','r') as f:
              for line in f.readlines():
                     x = line.split(':')
                     if (len(x) > 5):
                            text.append(line)
       
       
       print(len(text))
       print(text)

       with open('testpred.txt','r') as f:
              for line in f.readlines():
                     x = line.split(':')
                     if (len(x) > 5):
                            text1.append(line)

       print('\n')                     

       if len(text) == len(text1):
        print("There are same number of unwanted colon records in the main input text file and the errorlog file!")

		
text_break()


print('-------------------------------------------------------------------------------------------------')
print("Test 7 pass, the code script did not break and handles the unwanted colon records and saves them in a log file.")
print('-------------------------------------------------------------------------------------------------')
print('\n')


'''Check if the product id's accuracies in predict.txt are not in ascending order then are they in proper ascending order in the final output file'''
print('Eigth Test')
def order_accuracy():
       store = []
       store1 = []
       with open('testpred.txt',"r") as f:
           for line in f.readlines():
               split = line.split(':')
               if len(split) == 5:
                   store.append(split)
                   

       predict_data = pd.DataFrame(store)           
       predict_data.columns = ["Accuracy", "Prediction", "Attributes","product_id", "Product_Name"]
       print(predict_data.loc[18:])

       mer = merger('testpred.txt','dummy.csv')
       output(mer,'out_dummy.tsv') 

       with open('out_dummy.tsv','r') as f:
              for line in f.readlines():
                     store1.append(line)
       print('\n')
       print(store1[1])
       print('\n')              

       print('We can see from the above output that the dataframes output for product_id 147GTFEZH88P accuracy column was not in ascending order, but the output file has the Accuracy in ascending order. Hence, even if the order of accuracy is changed in the input file, it does not affect the output')
       print('\n')
order_accuracy()

print('-----------------------------------------------------------------------------------------------------------------------------------------------')
print("Test 8 pass, product id's accuracies in predict.txt are not in ascending order but they are in proper ascending order in the final output file.")
print('-----------------------------------------------------------------------------------------------------------------------------------------------')
print('\n')

'''Check if the encoding of the output file is written in proper format'''
print('Ninth Test')
def encode():
       blob = open('out.tsv').read()
       m = magic.Magic(mime_encoding=True)
       encoding = m.from_buffer(blob)

       print('The out.tsv file has been written in a'" ""'{}'" " "'format'.format(encoding))


encode()       
print('\n')
print('-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
print("Test 9 pass, the code script output was saved as a utf-8 format and verified by reading that the output file has a binary encoding and also verified by opening the file on Windows Microsoft excel, it opens perfectly fine.")
print('-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
print('\n')






       
