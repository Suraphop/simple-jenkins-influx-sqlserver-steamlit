pipeline {
    agent any

    environment {
        PASS = credentials('registry-pass') 
    }
    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                sh "chmod +x ./jenkins/build/build.sh"
                sh '''
                    ./jenkins/build/build.sh

                '''
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
                sh "chmod +x ./jenkins/test/test.sh"
                sh '''
                    ./jenkins/test/test.sh

                '''
            }
        }
        stage('Push') {
            steps {
                echo 'push..'
                sh "chmod +x ./jenkins/push/push.sh"
                sh './jenkins/push/push.sh'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
                sh "chmod +x ./jenkins/deploy/deploy.sh"
                sh './jenkins/deploy/deploy.sh'
            }
        }
    }
}