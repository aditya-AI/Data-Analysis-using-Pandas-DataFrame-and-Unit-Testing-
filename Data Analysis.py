import sys
import pandas as pd
import time
import csv


t1 = time.time()
if len(sys.argv) == 4:
    if sys.argv[1].endswith('.txt') and sys.argv[2].endswith('.csv') and sys.argv[3].endswith('.tsv'):
            file1,file2,file3 = sys.argv[1:]
    else:
        print('\n')
        print('Please provide the input in a correct manner. The .txt file then the .csv file and finally the output file .tsv! Make sure that each file is surrounded by Double Quotes')
        print('\n')    
else:
    print ('You failed to provide  all the input files on the command line!')
    print('\n')
    sys.exit(1) 
    


def text_data(filename):
        
        store = []
        store1 = []
        with open(filename,"r") as f:
            for line in f.readlines():
                split = line.split(':')
                try:
                    assert len(split)==5
                    store.append(split)
                except AssertionError:
                    store1.append(line)
                    #split[-2] = "".join(split[-2:])
                    #split.remove(split[-1])
                    #store.append(split)
                    with open('errorlogtext.txt','w') as f:
                        for result in store1:
                            f.write(str(result))

        
        predict_data = pd.DataFrame(store)          
        predict_data.columns = ["Accuracy", "Prediction", "Attributes","product_id", "Product_Name"]
        predict_data = predict_data.sort_values(by=['Accuracy'],ascending=[True])
        predict_data = predict_data.groupby('product_id').agg(lambda x: tuple(x))
        predict_data = predict_data.reset_index()
        del predict_data['Attributes']
        product_type = pd.DataFrame()
        product_type[["product_type_guess_3", "product_type_guess_2", "product_type_guess_1"]] = pd.DataFrame([x for x in predict_data.Prediction])
        confidence = pd.DataFrame()
        confidence[["Confidence_3", "Confidence_2", "Confidence_1"]] = pd.DataFrame([x for x in predict_data.Accuracy])
        prod_con = pd.concat([product_type, confidence], axis=1)
        predict_data = pd.concat([predict_data, prod_con], axis=1)
        del predict_data['Prediction']
        del predict_data['Accuracy']
        return predict_data
       


def csv_data(file):
    fields = ['charPath', 'primary_image_hash','primary_url','product_id','product_type_id','title']
    csv_data = pd.read_csv(file,delimiter='\t',quotechar="'",usecols=fields,warn_bad_lines=True, error_bad_lines=False)
    '''   
    RECOMMENDED = 6
    with open(file) as csv_file:
        reader = csv.reader(csv_file, delimiter='\t')
        for row in reader:
            if (len(row) != RECOMMENDED):
                with open('errorlogcsv.txt','w') as f:
                        for result in row:
                            f.write(str(result))
                           ''' 
    csv_data['charPath'] = csv_data['charPath'].apply(lambda x: x.replace('\\''','').replace('[[','').replace(']]','').replace('"', '').split('\\')[0][1:-1])
    csv_data["charPath"] = csv_data.charPath.apply(lambda x : ">".join(x.split(",")))
    return csv_data

def merger(filename,file):
    merge = pd.DataFrame()
    merge = pd.merge(text_data(filename),csv_data(file), on ='product_id',how='inner')
    merge = merge[['product_id','product_type_id','charPath','product_type_guess_3','Confidence_3','product_type_guess_2','Confidence_2','product_type_guess_1','Confidence_1','title','primary_url']]
    merge.columns = ['product_ids','product_type_id_1','product_type_name_1','product_type_guess_3','Confidence_3','product_type_guess_2','Confidence_2','product_type_guess_1','Confidence_1','title','image_url']
    merge = merge.sort_values(['Confidence_1','Confidence_2','Confidence_3'])
    return merge



try:
    mer =  merger(file1,file2)
except NameError:
    print('\n')
    print('Input file 1 and file 2 were not inputted correctly and so the function call is failed!')

def output(mer,file3):
    mer.to_csv(file3, sep='\t',index=False,encoding = 'utf-8')   

try:
    output(mer,file3) 
except NameError:
    print('\n')
    print('The output function failed to save the output in a .tsv file since the input files were not in a correct manner as expected')


t2 = time.time()
print(t2-t1)        
