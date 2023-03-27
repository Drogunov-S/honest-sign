package ru.drogunov;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.concurrent.TimedSemaphore;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.*;

public class App {
    public static void main(String[] args) {
        String token = "1cecc8fb-fb47-4c8a-af3d-d34c1ead8c4f";
        CrptApi crptApi = new CrptApi(TimeUnit.MINUTES, 2);
        //crptApi.introduceGoods(introduceGoodsRfDocs, token);
    }
}

class CrptApi {
    private final Worker worker;
    //TODO Нету полных данных для формирования корректного URL, методы проверены
    private final URI uri = URI.create("http://<server-name>[:server-port]/api/v2/{extension}/rollout?omsId={omsId}");
//    private final URI uri = URI.create("http://localhost:8889/api/v2/prod");
    
    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.worker = new Worker(timeUnit, requestLimit);
    }
    
    public void introduceGoods(IntroduceGoodsRfDocs doc, String token) {
        IntroduceGoodsTask task = new IntroduceGoodsTask(doc, token, uri);
        worker.addTask(task);
        
        if (!worker.isStart()) {
            worker.start();
        }
    }
    
    public void stopQueue() {
        worker.close();
    }
}

@Getter
class Worker implements Runnable {
    private final ConcurrentLinkedQueue<IntroduceGoodsTask> queue = new ConcurrentLinkedQueue<>();
    private final TimedSemaphore semaphore;
    private final ExecutorService service = Executors.newFixedThreadPool(2);
    private final ArrayList<Future<HttpResponse<String>>> results = new ArrayList<>();
    private boolean interrupted = false;
    private boolean start = false;
    
    Worker(TimeUnit timeUnit, int requestLimit) {
        this.semaphore = new TimedSemaphore(1, timeUnit, requestLimit);
    }
    
    public void start() {
        this.start = true;
        new Thread(this).start();
    }
    
    @Override
    public void run() {
        if (interrupted) {
            service.shutdown();
            return;
        }
        while (true) {
            if (queue.size() > 0 && semaphore.tryAcquire()) {
                Future<HttpResponse<String>> future = service.submit(Objects.requireNonNull(queue.poll()));
                results.add(future);
            }
        }
    }
    
    public void addTask(IntroduceGoodsTask task) {
        queue.add(task);
    }
    
    public void close() {
        interrupted = true;
    }
    
}

class IntroduceGoodsTask implements Callable<HttpResponse<String>> {
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final URI url;
    private final IntroduceGoodsRfDocs doc;
    private final String token;
    
    IntroduceGoodsTask(IntroduceGoodsRfDocs doc, String token, URI uri) {
        this.doc = doc;
        this.token = token;
        this.url = uri;
    }
    
    @Override
    public HttpResponse<String> call() throws Exception {
        String body = mapper.writeValueAsString(doc);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(url)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-type", "application/json")
                .header("clientToken", token)
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}

@Getter
@Setter
class IntroduceGoodsRfDocs implements Serializable {
    private Description description;
    private String docid;
    private String docStatus;
    private String docType;
    private boolean importRequest;
    private String ownerInn;
    private String participantInn;
    private String producerInn;
    @JsonFormat(pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
    private LocalDate productionDate;
    private ProductType productionType;
    private List<Product> products;
    @JsonFormat(pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
    private LocalDate regDate;
    private String regNumber;
    
    public IntroduceGoodsRfDocs(String docid, String docStatus, String docType, String ownerInn, String participantInn, String producerInn, LocalDate productionDate, ProductType productionType, LocalDate regDate) {
        this.docid = docid;
        this.docStatus = docStatus;
        this.docType = docType;
        this.ownerInn = ownerInn;
        this.participantInn = participantInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.productionType = productionType;
        this.regDate = regDate;
    }
    
    public IntroduceGoodsRfDocs(Description description, String docid, String docStatus, String docType, boolean importRequest, String ownerInn, String participantInn, String producerInn, LocalDate productionDate, ProductType productionType, List<Product> products, LocalDate regDate, String regNumber) {
        this.description = description;
        this.docid = docid;
        this.docStatus = docStatus;
        this.docType = docType;
        this.importRequest = importRequest;
        this.ownerInn = ownerInn;
        this.participantInn = participantInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.productionType = productionType;
        this.products = products;
        this.regDate = regDate;
        this.regNumber = regNumber;
    }
}

enum ProductType {
    OWN_PRODUCTION,
    CONTRACT_PRODUCTION
}

@Getter
@Setter
class Description {
    private String participantInn;
    
    public Description(String participantInn) {
        this.participantInn = participantInn;
    }
    
}

@Getter
@Setter
class Product {
    private CertificateDocument certificateDocument;
    @JsonFormat(pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
    private LocalDate certificateDocumentDate;
    private String certificateDocumentNumber;
    private String ownerInn;
    private String producerInn;
    @JsonFormat(pattern = "yyyy-MM-dd", timezone = "Europe/Moscow")
    private LocalDate productionDate;
    private String tnvedCode;
    private String uitCode;
    private String uituCode;
    
    public Product(String ownerInn, String producerInn, LocalDate productionDate, String tnvedCode) {
        this.ownerInn = ownerInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.tnvedCode = tnvedCode;
    }
    
    public Product(CertificateDocument certificateDocument, LocalDate certificateDocumentDate, String certificateDocumentNumber, String ownerInn, String producerInn, LocalDate productionDate, String tnvedCode, String uitCode, String uituCode) {
        this.certificateDocument = certificateDocument;
        this.certificateDocumentDate = certificateDocumentDate;
        this.certificateDocumentNumber = certificateDocumentNumber;
        this.ownerInn = ownerInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.tnvedCode = tnvedCode;
        this.uitCode = uitCode;
        this.uituCode = uituCode;
    }
    
}

enum CertificateDocument {
    CONFORMITY_CERTIFICATE,
    CONFORMITY_DECLARATION
}
