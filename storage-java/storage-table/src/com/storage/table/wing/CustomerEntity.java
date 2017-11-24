package com.storage.table.wing;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class CustomerEntity extends TableServiceEntity {
    public CustomerEntity(String lastName, String firstName) {
        this.partitionKey = lastName;
        this.rowKey = firstName;
    }

    public CustomerEntity() { }

    CustomerEntityData data = new CustomerEntityData();

	public String getEmail() {
        return this.data.email;
    }

    public void setEmail(String email) {
        this.data.email = email;
    }

    public String getPhoneNumber() {
        return this.data.phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.data.phoneNumber = phoneNumber;
    }
}