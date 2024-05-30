## Uruchomienie

1. Uruchom cluster za pomocą komendy:
    ```shell
    gcloud dataproc clusters create ${CLUSTER_NAME} \
    --enable-component-gateway --region ${REGION} --subnet default \
    --master-machine-type n1-standard-4 --master-boot-disk-size 50 \
    --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
    --image-version 2.1-debian11 --optional-components ZOOKEEPER,DOCKER,FLINK \
    --project ${PROJECT_ID} --max-age=3h \
    --metadata "run-on-master=true" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
    ```
2. Wrzuć do swojego bucketa pliki z danymi (nie zmieniaj ich domyślnej nazwy)
3. Uruchom 3 terminale SSH na masterze klastra
4. Wrzuć plik zip z projektem korzystając z jednego z terminali
5. Wykonaj komendy:
    ```shell
    unzip projekt2.zip
    chmod +x *.sh
    ```
6. Uruchom skrypt `./reset.sh` w celu zresetowania środowiska
   ![Wynik skryptu resetującego](wynik-skryptu-resetujacego.png)
7. Uruchom skrypt `./produce.sh` w celu rozpoczęcia generowania danych

### Uruchomienia przetwarzania

W celu uruchomienia przetwarzania należy wykonać skrypt `./run.sh` w jednym z terminali. Skrypt ten można parametryzować
za pomocą flag:

- `--flink-delay` - tryb obsługi utrzymania obrazy czasu rzeczywistego (domyślnie `C`, możliwe `A` lub `C`)
- `--flink-anomaly-period` - długość okresu przy detekcji anomalii, wyrażona w dniach (domyślnie `30`)
- `--flink-anomaly-threshold` - minimalny próg przestępstw rejestrowanych przez FBI przy detekcji anomalii, wyrażony w
  procentach (domyślnie `60`)

#### Przykładowe uruchomienie

- wariant 1 - tryb obsługi A, anomalie nie występują
    ```shell
    ./run.sh --flink-delay A --flink-anomaly-period 30 --flink-anomaly-threshold 110
    ```
- wariant 2 - tryb obsługi C, anomalie występują często
    ```shell
    ./run.sh --flink-delay C --flink-anomaly-period 30 --flink-anomaly-threshold 50
    ```

#### Uruchomienie z punktami kontrolnymi

W celu uruchomienia przetwarzania ze stanem z punktu kontrolnego należy wykonać skrypt `./run-chekpoint.sh`, gdzie
pierwszym argumentem jest nazwa katalogu z punktem kontrolnym (najczęściej najświeższy katalog uzyskany za
pomocą `hadoop fs -ls /tmp/flink-checkpoints`).