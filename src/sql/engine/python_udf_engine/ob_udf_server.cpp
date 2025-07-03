#include "ob_udf_scheduler.h"
#include "ob_shared_memory_manager.h"
#include <Python.h>
#include <arrow/python/pyarrow.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

using namespace oceanbase;
using namespace sql;

std::string python_error(){
	char buf[65536], *buf_p = buf;
  PyObject *type_obj, *value_obj, *traceback_obj;
  PyErr_Fetch(&type_obj, &value_obj, &traceback_obj);
  if (value_obj == NULL)
      return "";
  PyObject *pstr = PyObject_Str(value_obj);
  const char* value = PyUnicode_AsUTF8(pstr);
  size_t szbuf = sizeof(buf);
  int l;
  PyCodeObject *codeobj;
  l = snprintf(buf_p, szbuf, ("Error Message:\n%s"), value);
  buf_p += l;
  szbuf -= l;
  if (traceback_obj != NULL) {
      l = snprintf(buf_p, szbuf, ("\n\nTraceback:\n"));
      buf_p += l;
      szbuf -= l;
      PyTracebackObject *traceback = (PyTracebackObject *)traceback_obj;
      for (; traceback && szbuf > 0; traceback = traceback->tb_next) {
          //codeobj = traceback->tb_frame->f_code;
          codeobj = PyFrame_GetCode(traceback->tb_frame);
          l = snprintf(buf_p, szbuf, "%s: %s(# %d)\n",
              PyUnicode_AsUTF8(PyObject_Str(codeobj->co_name)),
              PyUnicode_AsUTF8(PyObject_Str(codeobj->co_filename)),
              traceback->tb_lineno);
          buf_p += l;
          szbuf -= l;
      }
  }
  return std::string(buf);
}

int main(int argc, char **argv)
{
	// 打开日志文件
	std::ofstream log_file("/home/obtest/log/server_err.log", std::ios::app);
	if (!log_file.is_open())
	{
		std::cerr << "[Server] Failed to open log file\n";
		return 1;
	}

	log_file << std::unitbuf;
	
	if (argc < 2)
	{
		log_file << "[Server] you should add a parameter\n";
		log_file.close();
		return 0;
	}
	std::string channel_name = argv[1];
	int channel_name_int = std::atoi(channel_name.c_str());
	int cmd;
	bool stop_flag = true;

	// 写入日志的辅助函数
	auto log = [&](std::string message, bool flag = true)
	{
		if(flag)
			log_file << "[Server] " << channel_name << " " << message << std::endl;
	};

	log("start server");

	if (!Py_IsInitialized())
	{
		Py_Initialize();
	}
	PyGILState_STATE gstate;
	gstate = PyGILState_Ensure();

	PyObject *dycacher = PyImport_ImportModule("dycacher");
	if (!dycacher)
	{
		log("Failed to import module 'dycacher'\n"+python_error(), true);
	}

	PyObject *reset_func = PyObject_GetAttrString(dycacher, "reset_cache");
	if (!reset_func)
	{
		log("Failed to get 'reset_cache' function from 'dycacher'\n"+python_error(), true);
		Py_DECREF(dycacher);
	}

	if (arrow::py::import_pyarrow())
	{
		log("import pyarrow error! Make sure your default python environment has installed the pyarrow", true);
		exit(0);
	}

	IMLaneScheduler scheduler(false, 0, channel_name_int);
	SharedMemoryManager shm_server(channel_name, ProcessKind::MANAGER);

	// prepare the environment
	std::ifstream file("/home/IMBridge/imlane_server/code.py");
	std::stringstream buffer;
	buffer << file.rdbuf();
	std::string python_code = buffer.str();
	PyRun_SimpleString(python_code.c_str());
	PyObject *main_module = PyImport_AddModule("__main__");
	PyObject *main_dict = PyModule_GetDict(main_module);
	PyObject *MyProcess = PyDict_GetItemString(main_dict, "MyProcess");
	PyObject *my_process_instance = PyObject_CallObject(MyProcess, NULL);
	if (my_process_instance == NULL)
	{
		log("Failed to create MyProcess instance\n"+python_error(), true);
		return 0;
	}
	scheduler.push_id_to_avaliable_queue(channel_name_int);
	log("prepare the environment");

	while (stop_flag)
	{
		cmd = scheduler.get_message_from_task_queue();
		switch (cmd)
		{
		case TASK_UDF_INFER:
		{
			log("start handle");
			// read table
			std::shared_ptr<arrow::Table> my_table;
			read_arrow_from_shared_memory(my_table, shm_server, INPUT_TABLE);

			// handle table and predict
			PyObject *py_table_tmp = arrow::py::wrap_table(std::move(my_table));
			PyObject *py_result = PyObject_CallMethod(my_process_instance, "process", "O", py_table_tmp);

			// get the result
			if (py_result != NULL)
			{
				my_table = arrow::py::unwrap_table(py_result).ValueOrDie();
			}
			else
			{
				log("Failed to process table\n" + python_error(), true);
				return 0;
			}
			log("handle finished");

			// write result to shared memory
			write_arrow_to_shared_memory(my_table, shm_server, OUTPUT_TABLE);
			shm_server.client_post();
			// TODO: update the avaliable queue
			shm_server.server_wait();
			log("end id");
			// scheduler.push_id_to_avaliable_queue(channel_name_int);
			break;
		}
		case TASK_DESTROY:
		{
			stop_flag = false;
			break;
		}
		case TASK_RESET_CACHE:
		{
			PyObject *res = PyObject_CallObject(reset_func, NULL);
			if (!res)
			{
				log("Failed to reset cache\n" + python_error(), true);
			}
			break;
		}
		default:
			log("Invalid command " + std::to_string(cmd), true);
			throw std::invalid_argument("[Server] Invalid command " + std::to_string(cmd));
		}
	}
	log("udf server closed");
	PyGILState_Release(gstate);
	Py_Finalize();
	log_file.close();
	return 0;
}