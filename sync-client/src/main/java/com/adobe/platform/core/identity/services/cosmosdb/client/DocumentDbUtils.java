package com.adobe.platform.core.identity.services.cosmosdb.client;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import io.vavr.control.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


public class DocumentDbUtils {
    private static final Logger log = LoggerFactory.getLogger(DocumentDbUtils.class.getSimpleName());

    DocumentDbUtils() {
    }





}