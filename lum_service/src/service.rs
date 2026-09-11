use std::{
    any::{self, TypeId},
    cmp::Ordering,
    future::Future,
    sync::Weak,
};

use dynosaur::dynosaur;
use lum_boxtypes::BoxedError;
use lum_event::Observable;

use super::{
    service_manager::ServiceManager,
    types::{Priority, Status},
};

#[derive(Debug)]
pub struct ServiceInfo {
    pub type_id: TypeId,
    pub type_name: &'static str,
    pub name: String,
    pub priority: Priority,

    pub status: Observable<Status>,
}

impl ServiceInfo {
    pub fn new(service_type: TypeId, name: impl Into<String>, priority: Priority) -> Self {
        let type_id = service_type;
        let type_name = any::type_name_of_val(&type_id);
        let name = name.into();
        let status = Observable::new(Status::Stopped, format!("{type_name}::status_change"));

        Self {
            type_id,
            type_name,
            name,
            priority,
            status,
        }
    }
}

impl PartialEq for ServiceInfo {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for ServiceInfo {}

impl Ord for ServiceInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for ServiceInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[dynosaur(pub DynService = dyn(box) Service)]
pub trait Service: Send + Sync {
    fn info(&self) -> &ServiceInfo;
    fn info_mut(&mut self) -> &mut ServiceInfo;

    fn as_any(&self) -> &dyn any::Any;
    fn as_any_mut(&mut self) -> &mut dyn any::Any;

    fn start(
        &mut self,
        service_manager: Weak<ServiceManager>,
    ) -> impl Future<Output = Result<(), BoxedError>> + Send + '_;
    fn stop(&mut self) -> impl Future<Output = Result<(), BoxedError>> + Send + '_;

    fn fail(&mut self, _message: &str) -> impl Future<Output = ()> + Send {
        Box::pin(async move {})
    }

    fn is_available(&self) -> bool {
        self.info().status.get() == Status::Started
    }
}

impl DynService<'_> {
    pub fn downcast_ref<T: Service + 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }

    pub fn downcast_mut<T: Service + 'static>(&mut self) -> Option<&mut T> {
        self.as_any_mut().downcast_mut::<T>()
    }
}

impl Eq for DynService<'_> {}

impl PartialEq for DynService<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.info() == other.info()
    }
}

impl Ord for DynService<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.info().cmp(other.info())
    }
}

impl PartialOrd for DynService<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
