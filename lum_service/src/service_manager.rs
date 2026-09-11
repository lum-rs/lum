use dashmap::DashMap;
use lum_boxtypes::{BoxedError, LifetimedPinnedBoxedFutureResult};
use lum_event::{
    EventRepeater,
    event_repeater::{AttachError, DetachError},
};
use lum_log::{error, error_panic, info, warn};
use thiserror::Error;
use tokio::{
    spawn,
    sync::{Mutex, MutexGuard},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    service::{Priority, ServiceInfo, Status},
    taskchain::Taskchain,
};

use super::service::{DynService, Service};

use std::{
    any::TypeId,
    collections::HashMap,
    fmt::{self, Display},
    future::Future,
    sync::{Arc, Weak},
    time::Duration,
};

pub type ServiceHandle = Arc<Mutex<Box<DynService<'static>>>>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum Health {
    Healthy,
    Unhealthy,
}

impl Display for Health {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Health::Healthy => write!(f, "Healthy"),
            Health::Unhealthy => write!(f, "Unhealthy"),
        }
    }
}

#[derive(Debug, Error)]
pub enum StartupError {
    #[error("Service {0} ({1}) is not managed by this Service Manager")]
    ServiceNotManaged(String, String),

    #[error("Service {0} ({1}) is not stopped")]
    ServiceNotStopped(String, String),

    //TODO: BackgroundTaskRunning(String, String, int32): Service {0} ({1}) has {2} background tasks running
    #[error("Service {0} ({1}) already has a background task running")]
    BackgroundTaskAlreadyRunning(String, String),

    #[error(
        "Failed to attach Service Manager's status_change EventRepeater to {0} ({1})'s status_change Event: {2}"
    )]
    StatusAttachmentFailed(String, String, AttachError),

    #[error("Service {0} ({1}) failed to start")]
    FailedToStartService(String, String),

    #[error("The ServiceManager has been dropped")]
    ServiceManagerDropped,
}

#[derive(Debug, Error)]
pub enum ShutdownError {
    #[error("Service {0} ({1}) is not managed by this Service Manager")]
    ServiceNotManaged(String, String),

    #[error("Service {0} ({1}) is not started")]
    ServiceNotStarted(String, String),

    #[error("Service {0} ({1}) failed to stop")]
    FailedToStopService(String, String),

    #[error(
        "Failed to detach Service Manager's status_change EventRepeater from {0} ({1})'s status_change Event: {2}"
    )]
    StatusDetachmentFailed(String, String, DetachError),

    #[error("The ServiceManager has been dropped")]
    ServiceManagerDropped,
}

#[derive(Debug, Error)]
pub enum RunTaskError {
    #[error("Service {0} ({1}) is not started or currently starting")]
    ServiceNotStarted(String, String),

    #[error("Service {0} ({1}) is not managed by this Service Manager")]
    ServiceNotManaged(String, String),

    #[error("The ServiceManager has been dropped")]
    ServiceManagerDropped,
}

#[derive(Debug, Error)]
pub enum ServiceManagerHandleError {
    #[error("The ServiceManager has been dropped.")]
    ServiceManagerDropped,
}

pub struct ServiceManagerInner {
    pub services: HashMap<TypeId, ServiceHandle>,
    pub on_status_change: Arc<EventRepeater<Status>>,

    background_tasks: DashMap<TypeId, Vec<JoinHandle<Result<(), BoxedError>>>>,
}

impl ServiceManagerInner {
    pub async fn start_service(
        &self,
        service: ServiceHandle,
        handle: ServiceManagerHandle,
    ) -> Result<(), StartupError> {
        let mut service_lock = service.lock().await;

        let service_info = service_lock.info();
        if !self.manages_service_by_type_id(&service_info.type_id) {
            return Err(StartupError::ServiceNotManaged(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
            ));
        }

        if service_info.status.get() != Status::Stopped {
            return Err(StartupError::ServiceNotStopped(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
            ));
        }

        if self.has_background_tasks_by_type_id(&service_info.type_id) {
            return Err(StartupError::BackgroundTaskAlreadyRunning(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
            ));
        }

        let service_status_event = service_info.status.on_change.handle();
        let attachment_result = self.on_status_change.attach(service_status_event);
        if let Err(err) = attachment_result {
            return Err(StartupError::StatusAttachmentFailed(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
                err,
            ));
        }

        self.init_service(&mut service_lock, handle).await?;
        info!("Started service {}", service_lock.info().name); // Reacquiring to allow above mutable borrow

        Ok(())
    }

    pub async fn start_services(
        &self,
        handle: ServiceManagerHandle,
    ) -> Vec<Result<(), StartupError>> {
        let mut results = Vec::new();
        for pair in &self.services {
            let service = pair.1.clone();
            let result = self.start_service(service, handle.clone()).await;

            results.push(result);
        }

        results
    }

    pub async fn stop_service(&self, service: ServiceHandle) -> Result<(), ShutdownError> {
        let mut service_lock = service.lock().await;

        let service_info = service_lock.info();
        if !(self.manages_service_by_type_id(&service_info.type_id)) {
            return Err(ShutdownError::ServiceNotManaged(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
            ));
        }

        if service_info.status.get() != Status::Started {
            return Err(ShutdownError::ServiceNotStarted(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
            ));
        }

        self.shutdown_service(&mut service_lock).await?;

        //TODO: Find better way to handle this
        // Reacquiring to allow above mutable borrow
        let service_info = service_lock.info();

        let service_status_event = service_info.status.on_change.handle();
        let detach_result = self.on_status_change.detach(service_status_event);
        if let Err(err) = detach_result {
            return Err(ShutdownError::StatusDetachmentFailed(
                service_info.name.to_string(),
                service_info.type_name.to_string(),
                err,
            ));
        }

        info!("Stopped service {}", service_info.name);

        Ok(())
    }

    pub async fn stop_services(&self) -> Vec<Result<(), ShutdownError>> {
        let mut results = Vec::new();
        for pair in &self.services {
            let service = pair.1.clone();
            let result = self.stop_service(service).await;

            results.push(result);
        }

        results
    }

    pub async fn get_service_by_type<T: Service + 'static>(&self) -> Option<ServiceHandle> {
        for service in self.services.values() {
            let lock = service.lock().await;
            if lock.downcast_ref::<T>().is_some() {
                return Some(Arc::clone(service));
            }
        }
        None
    }

    pub async fn with_service<T: Service + 'static, R>(
        &self,
        f: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        match self.get_service_by_type::<T>().await {
            Some(service) => {
                let mut lock = service.lock().await;
                let service_ref = lock.downcast_mut::<T>().unwrap();

                Some(f(service_ref))
            }
            None => None,
        }
    }

    pub async fn with_service_async<T: Service + 'static, R, F: Future<Output = R>>(
        &self,
        f: impl FnOnce(&mut T) -> F,
    ) -> Option<R> {
        match self.get_service_by_type::<T>().await {
            Some(service) => {
                let mut lock = service.lock().await;
                let service_ref = lock.downcast_mut::<T>().unwrap();

                Some(f(service_ref).await)
            }
            None => None,
        }
    }

    pub fn get_service(&self, type_id: &TypeId) -> Option<ServiceHandle> {
        self.services.get(type_id).map(Arc::clone)
    }

    pub fn manages_service_by_type_id(&self, type_id: &TypeId) -> bool {
        self.get_service(type_id).is_some()
    }

    pub async fn manages_service(&self, service: &ServiceHandle) -> bool {
        let type_id = service.lock().await.info().type_id;
        self.manages_service_by_type_id(&type_id)
    }

    pub fn has_background_tasks_by_type_id(&self, type_id: &TypeId) -> bool {
        self.background_tasks.contains_key(type_id)
    }

    pub fn has_background_tasks_by_mutex_guard(
        &self,
        service: &MutexGuard<'_, Box<DynService<'static>>>,
    ) -> bool {
        let type_id = service.info().type_id;
        self.has_background_tasks_by_type_id(&type_id)
    }

    pub async fn has_background_tasks(&self, service: &ServiceHandle) -> bool {
        let type_id = service.lock().await.info().type_id;
        self.has_background_tasks_by_type_id(&type_id)
    }

    //TODO: When Rust allows async closures, refactor this to use iterator methods instead of for loop
    pub async fn health(&self) -> Health {
        for pair in self.services.iter() {
            let service = pair.1;
            let service_lock = service.lock().await;
            let service_info = service_lock.info();

            if service_info.priority != Priority::Essential {
                continue;
            }

            let status = service_info.status.get();
            if status != Status::Started {
                return Health::Unhealthy;
            }
        }

        Health::Healthy
    }

    //TODO: Remove?
    //TODO: When Rust allows async closures, refactor this to use iterator methods instead of for loop
    pub async fn status_overview(&self) -> String {
        let mut text_buffer = String::new();

        let mut failed_essentials = Vec::new();
        let mut failed_optionals = Vec::new();
        let mut non_failed_essentials = Vec::new();
        let mut non_failed_optionals = Vec::new();
        let mut others = Vec::new();

        for pair in self.services.iter() {
            let service = pair.1;
            let lock = service.lock().await;
            let info = lock.info();
            let status = info.status.get();
            let priority = info.priority;
            let name = info.name.as_str();

            match status {
                Status::Started | Status::Stopped => match priority {
                    Priority::Essential => {
                        non_failed_essentials.push(format!(" - {name}: {status}"));
                    }
                    Priority::Optional => {
                        non_failed_optionals.push(format!(" - {name}: {status}"));
                    }
                },
                Status::FailedToStart(_) | Status::FailedToStop(_) | Status::RuntimeError(_) => {
                    match priority {
                        Priority::Essential => {
                            failed_essentials.push(format!(" - {name}: {status}"));
                        }
                        Priority::Optional => {
                            failed_optionals.push(format!(" - {name}: {status}"));
                        }
                    }
                }
                _ => {
                    others.push(format!(" - {name}: {status}"));
                }
            }
        }

        if !failed_essentials.is_empty() {
            text_buffer.push_str("Failed essential services:\n");
            text_buffer.push_str(failed_essentials.join("\n").as_str());
        }

        if !failed_optionals.is_empty() {
            text_buffer.push_str("Failed optional services:\n");
            text_buffer.push_str(failed_optionals.join("\n").as_str());
        }

        if !non_failed_essentials.is_empty() {
            text_buffer.push_str("Essential services:\n");
            text_buffer.push_str(non_failed_essentials.join("\n").as_str());
        }

        if !non_failed_optionals.is_empty() {
            text_buffer.push_str("Optional services:\n");
            text_buffer.push_str(non_failed_optionals.join("\n").as_str());
        }

        if !others.is_empty() {
            text_buffer.push_str("Other services:\n");
            text_buffer.push_str(others.join("\n").as_str());
        }

        let longest_width = text_buffer
            .lines()
            .map(|line| line.len())
            .max()
            .unwrap_or(0);

        let mut headline = String::from("Status overview\n");
        headline.push_str("─".repeat(longest_width).as_str());
        headline.push('\n');
        text_buffer.insert_str(0, &headline);

        text_buffer
    }

    async fn init_service(
        &self,
        service: &mut MutexGuard<'_, Box<DynService<'static>>>,
        handle: ServiceManagerHandle,
    ) -> Result<(), StartupError> {
        service.info_mut().status.set(Status::Starting).await;
        let start = service.start(handle);
        let timeout_result = timeout(Duration::from_secs(10), start).await; //TODO: Add to config instead of hardcoding duration

        //TODO: Merge all cases into enum with variants "Ok", "Err", and "Timeout"
        let service_info = service.info_mut();
        match timeout_result {
            Ok(start_result) => match start_result {
                Ok(()) => {
                    service_info.status.set(Status::Started).await;
                }
                Err(error) => {
                    service_info
                        .status
                        .set(Status::FailedToStart(error.to_string()))
                        .await;

                    return Err(StartupError::FailedToStartService(
                        service_info.name.clone(),
                        service_info.type_name.to_string(),
                    ));
                }
            },
            Err(error) => {
                service_info
                    .status
                    .set(Status::FailedToStart(error.to_string()))
                    .await;

                return Err(StartupError::FailedToStartService(
                    service_info.name.clone(),
                    service_info.type_name.to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn shutdown_service(
        &self,
        service: &mut MutexGuard<'_, Box<DynService<'static>>>,
    ) -> Result<(), ShutdownError> {
        service.info_mut().status.set(Status::Stopping).await;
        self.abort_background_tasks(service).await;
        let stop = service.stop();
        let timeout_result = timeout(Duration::from_secs(10), stop).await; //TODO: Add to config instead of hardcoding duration

        //TODO: Merge all cases into enum with variants "Ok", "Err", and "Timeout"
        let service_info = service.info_mut();
        match timeout_result {
            Ok(stop_result) => match stop_result {
                Ok(()) => {
                    service_info.status.set(Status::Stopped).await;
                }
                Err(error) => {
                    service_info
                        .status
                        .set(Status::FailedToStop(error.to_string()))
                        .await;

                    return Err(ShutdownError::FailedToStopService(
                        service_info.name.clone(),
                        service_info.type_name.to_string(),
                    ));
                }
            },
            Err(error) => {
                service_info
                    .status
                    .set(Status::FailedToStop(error.to_string()))
                    .await;

                return Err(ShutdownError::FailedToStopService(
                    service_info.name.clone(),
                    service_info.type_name.to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn fail_service<IntoString: Into<String>>(
        &self,
        service: ServiceHandle,
        message: IntoString,
    ) {
        let mut service_lock = service.lock().await;
        self.fail_service_by_mutex_guard(&mut service_lock, message)
            .await;
    }

    async fn fail_service_by_mutex_guard<IntoString: Into<String>>(
        &self,
        service: &mut MutexGuard<'_, Box<DynService<'static>>>,
        message: IntoString,
    ) {
        service.info_mut().status.set(Status::Failing).await;
        self.abort_background_tasks(service).await;

        let message = message.into();
        service.fail(&message).await;
        service
            .info_mut()
            .status
            .set(Status::RuntimeError(message))
            .await;
    }

    pub async fn run_task(
        &self,
        service_info: &ServiceInfo,
        task: LifetimedPinnedBoxedFutureResult<'static, ()>,
        handle: ServiceManagerHandle,
    ) -> Result<(), RunTaskError> {
        // We're cloning these values to move them into the task's closure
        // Otherwise, we would reference service_info and get lifetime issues
        let service_name = service_info.name.to_string();
        let service_type_id = service_info.type_id;
        let service_type_name = service_info.type_name; // this is static, so no need to clone
        let service_status = service_info.status.get();

        if service_status != Status::Starting && service_status != Status::Started {
            return Err(RunTaskError::ServiceNotStarted(
                service_name.to_string(),
                service_type_name.to_string(),
            ));
        }

        let mut taskchain = Taskchain::new(task);
        //TODO: When Rust allows async closures, refactor this to have the "async" keyword after the "move" keyword
        taskchain.append(move |result| async move {
            let handle = handle;
            let inner = match handle.inner.upgrade() {
                Some(inner) => inner,
                None => {
                    error_panic!(
                        "A task of a service {service_name} ({service_type_name}) unexpectedly ended, but cannot mark service as failed because its corresponding ServiceManager was already dropped. Panicking to prevent further undefined behavior."
                    );
                }
            };

            let service = match inner.get_service(&service_type_id) {
                Some(service) => service,
                None => {
                    error_panic!(
                        "A task of a service {service_name} ({service_type_name}) unexpectedly ended, but no service with that ID was registered in its corresponding ServiceManager. Was it removed while the task was running? Panicking to prevent further undefined behavior."
                    );
                }
            };

            match result {
                Ok(()) => {
                    error!(
                        "A task of service {service_name} ({service_type_name}) ended unexpectedly! Service will be marked as failed."
                    );

                    inner.fail_service(service, "Background task ended unexpectedly!").await;
                }

                Err(error) => {
                    error!(
                        "A task of service {service_name} ({service_type_name}) ended with error: {error}. Service will be marked as failed.",
                    );

                    inner.fail_service(service, error.to_string()).await;
                }
            }
            Ok(())
        });

        let join_handle = spawn(taskchain.run());

        self.background_tasks
            .entry(service_info.type_id)
            .or_default()
            .push(join_handle);

        Ok(())
    }

    async fn abort_background_tasks(
        &self,
        service_lock: &MutexGuard<'_, Box<DynService<'static>>>,
    ) {
        let service_type_id = service_lock.info().type_id;

        if !self.has_background_tasks_by_type_id(&service_type_id) {
            return;
        }

        let tasks = self.background_tasks.get_mut(&service_type_id).unwrap();
        for task in tasks.iter() {
            task.abort();
        }
        self.background_tasks.remove(&service_type_id);
    }
}

impl Display for ServiceManagerInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Services: ")?;

        if self.services.is_empty() {
            write!(f, "None")?;
            return Ok(());
        }

        let mut services = self.services.iter().peekable();
        while let Some((_, service)) = services.next() {
            let service = service.blocking_lock();
            let service_info = service.info();

            write!(f, "{}", service_info.name,)?;
            if services.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServiceManagerHandle {
    inner: Weak<ServiceManagerInner>,
}

impl ServiceManagerHandle {
    pub fn is_dropped(&self) -> bool {
        self.inner.strong_count() == 0
    }

    pub fn try_with<R>(
        &self,
        func: impl FnOnce(&ServiceManagerInner) -> R,
    ) -> Result<R, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = func(&inner);
        Ok(result)
    }

    pub async fn try_with_async<R>(
        &self,
        func: impl AsyncFnOnce(&ServiceManagerInner) -> R,
    ) -> Result<R, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = func(&inner).await;
        Ok(result)
    }

    pub async fn start_service(&self, service: ServiceHandle) -> Result<(), StartupError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(StartupError::ServiceManagerDropped)?;

        inner.start_service(service, self.clone()).await
    }

    pub async fn start_services(
        &self,
    ) -> Result<Vec<Result<(), StartupError>>, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.start_services(self.clone()).await;
        Ok(result)
    }

    pub async fn stop_service(&self, service: ServiceHandle) -> Result<(), ShutdownError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ShutdownError::ServiceManagerDropped)?;

        inner.stop_service(service).await
    }

    pub async fn stop_services(
        &self,
    ) -> Result<Vec<Result<(), ShutdownError>>, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.stop_services().await;
        Ok(result)
    }

    pub async fn run_task(
        &self,
        service_info: &ServiceInfo,
        task: LifetimedPinnedBoxedFutureResult<'static, ()>,
    ) -> Result<(), RunTaskError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(RunTaskError::ServiceManagerDropped)?;

        inner.run_task(service_info, task, self.clone()).await
    }

    pub async fn get_service_by_type<T: Service + 'static>(
        &self,
    ) -> Result<Option<ServiceHandle>, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.get_service_by_type::<T>().await;
        Ok(result)
    }

    pub async fn with_service<T: Service + 'static, R>(
        &self,
        f: impl FnOnce(&mut T) -> R,
    ) -> Result<Option<R>, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.with_service(f).await;
        Ok(result)
    }

    pub async fn with_service_async<T: Service + 'static, R, F: Future<Output = R>>(
        &self,
        f: impl FnOnce(&mut T) -> F,
    ) -> Result<Option<R>, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.with_service_async(f).await;
        Ok(result)
    }

    pub fn get_service(
        &self,
        type_id: &TypeId,
    ) -> Result<Option<ServiceHandle>, ServiceManagerHandleError> {
        self.try_with(|inner| inner.get_service(type_id))
    }

    pub fn manages_service_by_type_id(
        &self,
        type_id: &TypeId,
    ) -> Result<bool, ServiceManagerHandleError> {
        self.try_with(|inner| inner.manages_service_by_type_id(type_id))
    }

    pub async fn manages_service(
        &self,
        service: &ServiceHandle,
    ) -> Result<bool, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.manages_service(service).await;
        Ok(result)
    }

    pub fn has_background_tasks_by_type_id(
        &self,
        type_id: &TypeId,
    ) -> Result<bool, ServiceManagerHandleError> {
        self.try_with(|inner| inner.has_background_tasks_by_type_id(type_id))
    }

    pub fn has_background_tasks_by_mutex_guard(
        &self,
        service: &MutexGuard<'_, Box<DynService<'static>>>,
    ) -> Result<bool, ServiceManagerHandleError> {
        self.try_with(|inner| inner.has_background_tasks_by_mutex_guard(service))
    }

    pub async fn has_background_tasks(
        &self,
        service: &ServiceHandle,
    ) -> Result<bool, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.has_background_tasks(service).await;
        Ok(result)
    }

    pub async fn health(&self) -> Result<Health, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.health().await;
        Ok(result)
    }

    pub async fn status_overview(&self) -> Result<String, ServiceManagerHandleError> {
        let inner = self
            .inner
            .upgrade()
            .ok_or(ServiceManagerHandleError::ServiceManagerDropped)?;

        let result = inner.status_overview().await;
        Ok(result)
    }
}

pub struct ServiceManager {
    inner: Arc<ServiceManagerInner>,
}

impl ServiceManager {
    //TODO: Do not take services on new(), add a manage(service: ServiceHandle) method instead
    pub async fn new(services: Vec<ServiceHandle>) -> Self {
        let mut services_map: HashMap<TypeId, ServiceHandle> = HashMap::new(); //TODO: Drop type annotation

        //TODO: When Rust allows async closures, refactor this to use iterator methods instead of for loop
        for service in services.into_iter() {
            let service_lock = service.lock().await;
            let service_info = service_lock.info();

            let existing_service = services_map.get(&service_info.type_id);
            if let Some(existing_service) = existing_service {
                let existing_service_lock = existing_service.lock().await;
                let existing_service_info = existing_service_lock.info();

                warn!(
                    "ServiceManager::new() was given service {} ({}), which has the same TypeId as service {} ({}). This is not allowed. The service {} ({}) will be ignored.",
                    service_info.name,
                    service_info.type_name,
                    existing_service_info.name,
                    existing_service_info.type_name,
                    service_info.name,
                    service_info.type_name
                );
                continue;
            }

            services_map.insert(service_info.type_id, service.clone());
        }

        let inner = ServiceManagerInner {
            services: services_map,
            background_tasks: DashMap::new(),
            on_status_change: Arc::new(EventRepeater::new("ServiceManager::on_status_change")),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn handle(&self) -> ServiceManagerHandle {
        ServiceManagerHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub async fn start_service(&self, service: ServiceHandle) -> Result<(), StartupError> {
        self.inner.start_service(service, self.handle()).await
    }

    pub async fn start_services(&self) -> Vec<Result<(), StartupError>> {
        self.inner.start_services(self.handle()).await
    }

    pub async fn stop_service(&self, service: ServiceHandle) -> Result<(), ShutdownError> {
        self.inner.stop_service(service).await
    }

    pub async fn stop_services(&self) -> Vec<Result<(), ShutdownError>> {
        self.inner.stop_services().await
    }

    pub async fn run_task(
        &self,
        service_info: &ServiceInfo,
        task: LifetimedPinnedBoxedFutureResult<'static, ()>,
    ) -> Result<(), RunTaskError> {
        self.inner.run_task(service_info, task, self.handle()).await
    }

    pub async fn get_service_by_type<T: Service + 'static>(&self) -> Option<ServiceHandle> {
        self.inner.get_service_by_type::<T>().await
    }

    pub async fn with_service<T: Service + 'static, R>(
        &self,
        f: impl FnOnce(&mut T) -> R,
    ) -> Option<R> {
        self.inner.with_service(f).await
    }

    pub async fn with_service_async<T: Service + 'static, R, F: Future<Output = R>>(
        &self,
        f: impl FnOnce(&mut T) -> F,
    ) -> Option<R> {
        self.inner.with_service_async(f).await
    }

    pub fn get_service(&self, type_id: &TypeId) -> Option<ServiceHandle> {
        self.inner.get_service(type_id)
    }

    pub fn manages_service_by_type_id(&self, type_id: &TypeId) -> bool {
        self.inner.manages_service_by_type_id(type_id)
    }

    pub async fn manages_service(&self, service: &ServiceHandle) -> bool {
        self.inner.manages_service(service).await
    }

    pub fn has_background_tasks_by_type_id(&self, type_id: &TypeId) -> bool {
        self.inner.has_background_tasks_by_type_id(type_id)
    }

    pub fn has_background_tasks_by_mutex_guard(
        &self,
        service: &MutexGuard<'_, Box<DynService<'static>>>,
    ) -> bool {
        self.inner.has_background_tasks_by_mutex_guard(service)
    }

    pub async fn has_background_tasks(&self, service: &ServiceHandle) -> bool {
        self.inner.has_background_tasks(service).await
    }

    pub async fn health(&self) -> Health {
        self.inner.health().await
    }

    pub async fn status_overview(&self) -> String {
        self.inner.status_overview().await
    }
}

impl Display for ServiceManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}
