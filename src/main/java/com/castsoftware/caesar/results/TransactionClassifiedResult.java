package com.castsoftware.caesar.results;

import com.castsoftware.caesar.entities.transactions.Transaction;

import java.util.List;

public class TransactionClassifiedResult extends TransactionResult {

	public List<String> categories;

	public TransactionClassifiedResult(Transaction tn, List<String> categories) {
		super(tn);
		this.categories = categories;
	}
}
