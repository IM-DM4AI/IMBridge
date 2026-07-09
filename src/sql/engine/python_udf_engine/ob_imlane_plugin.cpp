#include "ob_imlane_plugin.h"
#include "imlane/cpp/dbend_arrow_lane.hpp"

// #include "identity_arrow_converter.hpp"

namespace dbend = IMLane::DBEnd;

namespace oceanbase {

    namespace sql {
        class IMLanePlugin::Impl {
            public:
                Impl() {
                    // Constructor implementation
                    runtime_context_ = new dbend::RuntimeContext();
                }
                ~Impl() {
                    // Destructor implementation
                    delete runtime_context_;
                }
            public:
                // Private members
                dbend::RuntimeContext* runtime_context_;
        };

        IMLanePlugin::IMLanePlugin()
            : impl_(new Impl()) {}

        IMLanePlugin::~IMLanePlugin() {
            delete impl_;
        }

        void IMLanePlugin::Setup() {
            impl_->runtime_context_->Setup();
        }
    }
}