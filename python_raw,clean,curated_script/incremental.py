import time
class augment:
    input_filename = "D:\\PROJECT_DATA\\script_file\\log_data_ip_request.txt"
    desti = "D:\\PROJECT_DATA\\script_file\\project-demo-processed-input.txt"
    count = 0
    incrementBy = 0
    input_file_object = open(input_filename,'r')  #open for open file r is mode as read
    file_object = open(desti, 'a')          #append mode

    def changer(self):
        self.input_file_object.seek(0)      #bring control back to beggining of the file
        for line_num, line in enumerate(self.input_file_object):    #index+value=enumerate line(value)
            last_digit = int(line.strip(" ").split(" ")[0].split(".")[-1])
            if (line_num + 1) % 10 == 0:
                temp_digit_changer = last_digit + self.incrementBy
                line = line.replace(str(last_digit), str(temp_digit_changer), 1)        #1 for 1st occurance
            if (line_num + 1) % 9 == 0:
                line = line.replace("GET", "POST", 1)
            self.count += 1
            print("number of records inserted:--> " + str(self.count))      #only display purpose
            self.file_object.write(line)
        self.incrementBy += 1

    def closer(self):
        self.input_file_object.close()
        self.file_object.close()
        print("file closed")

if __name__ == "__main__":
    #Number of records wanted in Lakhs

    Num_of_records_wanted_in_lakhs = int(input("Enter the number of records wanted(in Lakhs): "))
    # Number of times inp file to be accessed
    num_access = int((Num_of_records_wanted_in_lakhs * 100000) / 319)
    print(num_access)

    augment = augment()
    for i in range(num_access):
        print("reading for "+str(i+1)+" time")
        # time.sleep(1)
        augment.changer()

    augment.closer()

# Close the file
# file_object.close()

# print(count)

# change the column values as per req

# append to a new text file